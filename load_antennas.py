import time, signal, json, random, itertools, math, sys
import psycopg2 #for postgres DB access
import numpy as np
import scipy
import matplotlib.pyplot as plt

import util, config #local modules

def user_positions(line):
	"""Deprecated. Used for extracting antennas from sample only. Extracts the positions from a trajectory line in a STEM csv file. 
	Args:
		line: the linestring from the csv file
	Returns:
		A list of tuples ((lat,lon),1) (for each position in the trajectory)"""
	userid, sequence = util.parse_trajectory(line)

	result = []
	for lat, lon, t in sequence:
		result.append(((lat, lon), 1))

	return result

def count(item):
	"""Deprecated. Used for extracting antennas from sample only. Aggregates values by addition.
	Args:
		item: a tuple (key, values) where values is a sequence of numbers
	Returns:
		A tuple (key, sum), where sum is the sum of all values
	"""
	key, count = item
	return (key, sum(count))

def antenna_position(args):
	"""Parses a line of an antenna csv
	Args:
		args: a tuple (i, line), where i is the line number and line the a one-line string from the antenna csv
	Returns:
		A list [((cellid, lon, lat, srid),1)] where cellid is either parsed by util.parse_antenna() or if no specific id is available, 
		the linenumber is used as the antenna id and lon, lat are the coordinates of the antenna in the 
		coordinate system specified by srid"""

	i, line = args
	a = util.parse_antenna(line)
	if len(a) == 3:
		return [((i,) + a, 1)] #use line number as antenna id
	elif len(a) == 4:
		return [(a, 1)] #use parsed id as antenna id
	else:
		raise ValueError("parse_antenna() must return 3- or 4-tuple")

def upload_antenna(args):
	"""Uploads an antenna position to the database.
	Args:
		args: a tuple ((cellid, lon, lat, srid), values), where values is a list that is ignored and the other variables 
		are the attributes of the antenna
	"""

	key, values = args

	conn = util.db_connect()
	cur = conn.cursor()
	cell, lat, lon, srid = key
	sql = "	INSERT INTO ant_pos (id, lon, lat, geom) \
 			WITH p AS (SELECT ST_Transform(ST_SetSRID(ST_MakePoint(%(lat)s,%(lon)s),%(srid)s), 4326) AS point) \
			SELECT %(cell)s, ST_Y(p.point), ST_X(p.point), p.point FROM p;"
	cur.execute(sql, {"lat": lat, "lon": lon, "srid": srid, "cell": cell})
	conn.commit()

	return None

def calculate_voronoi():
	"""Calculates the Voronoi diagram for the antennas in the ant_pos table and uploads the resulting polygons to the voronoi table"""
	conn = util.db_connect()
	cur = conn.cursor()

	cur.execute("SELECT id, lat, lon FROM ant_pos")

	cells = []
	points = []
	for cell, lat, lon in cur.fetchall():
		cells.append(cell)
		points.append([lon, lat])

	#add fake extreme points to bound Voronoi diagram
	points.append([config.BBOX["top"] + 1.0, config.BBOX["left"]-1.0])
	points.append([config.BBOX["bottom"] - 1.0, config.BBOX["left"]-1.0])
	points.append([config.BBOX["top"] + 1.0, config.BBOX["right"]+1.0])
	points.append([config.BBOX["bottom"] - 1.0, config.BBOX["right"]+1.0])
	points = np.array([(x-config.BBOX["bottom"], y - config.BBOX["left"]) for x, y in points]) #transform close to 0 to prevent float precison errors

	from scipy.spatial import Voronoi
	vor = Voronoi(points, qhull_options="QJ")

	data = []
	for i, cell in enumerate(cells): #skip fake points
		vertices = [tuple(vor.vertices[v]) for v in vor.regions[vor.point_region[i]] if vor.point_region[i] >= 0 and v >= 0]
		if len(vertices) >= 3:
			linestr = "LINESTRING (" + ",".join([str(lat + config.BBOX["left"]) + " " + str(lon + config.BBOX["bottom"]) for lon, lat in vertices + [vertices[0]]]) + ")"
			data.append((cell, points[i][0] + config.BBOX["bottom"], points[i][1] + config.BBOX["left"], linestr))

	print("Uploading Voronoi partition...")
	mapper = util.ParMap(upload_voronoi)
	mapper(data)

def upload_voronoi(args):
	"""Uploads a Voronoi cell polygon to the DB.
	Args:
		args: a tuple (cell, lon, lat, geom), where cell is the cellid, lon/lat the original cell tower position and geom a postgis
		LINESTRING describing the Voronoi polygon
	"""
	cell, lon, lat, geom = args

	conn = util.db_connect()
	cur = conn.cursor()

	bboxpoints = [	(config.BBOX["top"], config.BBOX["left"]),
					(config.BBOX["top"], config.BBOX["right"]),
					(config.BBOX["bottom"], config.BBOX["right"]),
					(config.BBOX["bottom"], config.BBOX["left"])]
	bboxstr = "LINESTRING (" + ",".join([str(lat) + " " + str(lon) for lon, lat in bboxpoints + [bboxpoints[0]]]) + ")"

	sql = "	INSERT INTO voronoi (id, lon, lat, geom) \
			WITH bbox AS (SELECT ST_SetSRID(ST_MakePolygon(ST_GeomFromText(%(bbox)s)),4326) AS poly)\
		   	SELECT %(cell)s,%(lon)s,%(lat)s,\
		   	(ST_Dump(ST_Intersection(bbox.poly, ST_SetSRID(ST_MakePolygon(ST_GeomFromText(%(geom)s)),4326)))).geom FROM bbox;"
	cur.execute(sql, {"cell": cell, "lon": lon, "lat": lat, "geom": geom, "bbox": bboxstr})
	conn.commit()

def signal_handler(signal, frame):
	global mapper, request_stop
	if mapper:
		mapper.stop()
	request_stop = True
	print("Aborting (can take a minute)...")
	sys.exit(1)

request_stop = False
mapper = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating antenna table...")
	cur.execute(open("SQL/01_Loading/create_ant_pos.sql", 'r').read())
	conn.commit()

	#DEPRECATED: read antenna positions from STEM sample
	#Find unique antennas from sample
	#mapper = util.MapReduce(user_positions, count, num_workers = 4) #add flows 
	#unique_antennas = mapper(open(config.SAMPLE_FILENAME, 'r').readlines(), length = config.SAMPLE_SIZE)
	#print("Uploading to database...")
	#sql = "INSERT INTO ant_pos (lon, lat, geom) VALUES (%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326));"
	#cur.executemany(sql, [(lon, lat, lon, lat) for (lat, lon), count in unique_antennas])
	#conn.commit()

	#Read antennas from file
	#Count lines for status indicator
	linecount = 0
	for line in open(config.ANTENNA_FILE).xreadlines(): 
		linecount += 1

	#parse antennas
	mapper = util.MapReduce(antenna_position, upload_antenna, num_workers = 4) #add flows 
	antennas = mapper(enumerate(open(config.ANTENNA_FILE, 'r').readlines()), length = linecount, pipe = True)
	conn.commit()

	print("Creating Voronoi table...")
	cur.execute(open("SQL/01_Loading/create_voronoi.sql", 'r').read())
	conn.commit()

	print("Calculating Voronoi partition...")
	calculate_voronoi()

