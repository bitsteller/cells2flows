import time, signal, json, random, sys
from multiprocessing import Pool
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def extract_segments():
	"""Extract unique segements from all cellpaths in the trip table and inserts 
	into cellpath_parts table.
	"""
	global mconn, mcur

	print("Creating getParts() function...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_getparts_func.sql", 'r').read())
	mconn.commit()

	print("Creating cellpath_parts view (takes a while)...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_cellpath_parts.sql", 'r').read(), {"cells": tuple(config.CELLS)})
	mconn.commit()

def best_waypoint(segment):
	"""Finds the best waypoint in cell b when coming from cell a and heading to c and
	inserts it into waypoints table.
	Args:
		segment: a 3-tuple of cell ids in the order of travel"""
	global conn, cur

 	a,b,c = segment
	y = None #when no start or endpoint, no border points or no routes were found, null value will be added to the database

	#find start and end nodes
	sql = "	SELECT xid, ST_Y(x.geom) AS xlat, ST_X(x.geom) AS xlon, zid, ST_Y(z.geom) AS zlat, ST_X(z.geom) AS zlon\
			FROM closest_junction(%s, %s) AS xid, closest_junction(%s, %s) AS zid, hh_2po_4pgr_vertices AS x, hh_2po_4pgr_vertices AS z\
			WHERE x.id = xid AND z.id = zid"
	cur.execute(sql, (c,a,a,c))

	if cur.rowcount > 0: #start and end point found
		x, xlat, xlon, z, zlat, zlon = cur.fetchone()

		#fetch waypoint candidates
		sql = "	SELECT junction_id AS yid, ST_Y(y.geom) AS ylat, ST_X(y.geom) AS ylon\
				FROM boundary_junctions, hh_2po_4pgr_vertices AS y\
				WHERE antenna_id = %s AND y.id = boundary_junctions.junction_id"
		cur.execute(sql, (b,))
		y_candidates = cur.fetchall()

		#calculate route cost for all waypoints
		costs = []
		for y, ylat, ylon in y_candidates:
			costs.append(route_cost(xlat, xlon, ylat, ylon, zlat, zlon))
			#To see route, export gpx file:	print("\n".join(urllib2.urlopen('http://www.server.com:5000/viaroute?output=gpx&loc=' + str(xlat) + ',' + str(xlon) + '&loc=' + str(ylat) + ',' + str(ylon) + '&loc=' + str(zlat) + ',' + str(zlon)).readlines()))

		#select cheapest waypoint
		if len(costs) > 0 and min(costs) < float("inf"): #at least one feasible route found
			y = y_candidates[costs.index(min(costs))][0]

	cur.execute("INSERT INTO waypoints VALUES (%s,%s);", ([a,b,c], y))
	conn.commit()

def route_cost(xlat, xlon, ylat, ylon, zlat, zlon, attempts = 3):
	"""Calculates the cost from x via y to z; OSRM backend needs to listen at port 5000
	Args:
		xlat: latitude of the start point
		xlon: longitude of the start point
		ylat: latitude of the via point
		ylon: longitude of the via point
		zlat: latitude of the end point
		zlon: longitude of the end point
	Returns:
		The cost of the calculated route (travel time in seconds) or inf if no route found"""

	data = {}
	try:
		data = json.load(urllib2.urlopen('http://www.server.com:5000/viaroute?loc=' + str(xlat) + ',' + str(xlon) + '&loc=' + str(ylat) + ',' + str(ylon) + '&loc=' + str(zlat) + ',' + str(zlon)))
	except Exception, e:
		print("WARNING: " + e.message)
		if attempts > 0:
			route_cost(xlat, xlon, ylat, ylon, zlat, zlon, attempts = attempts - 1)
		else:
			raise e

	if "route_summary" in data:
		return data["route_summary"]["total_time"]
	else: #no feasible route found, return infinite cost
		return float("inf")

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

mapper = None
request_stop = False
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	mconn = util.db_connect()
	mcur = mconn.cursor()

	extract_segments()

	print("Creating waypoints table...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_waypoints.sql", 'r').read())
	mconn.commit()

	print("Creating closest_junction() function...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_closest_junction_func.sql", 'r').read())
	mconn.commit()

	#get number of remaining segments to calculate
	sql_remaining = "SELECT COUNT(*) FROM cellpath_parts WHERE NOT EXISTS(SELECT * FROM waypoints WHERE waypoints.part = cellpath_parts.part)"
	mcur.execute(sql_remaining)
	remaining = mcur.fetchone()[0]

	while remaining > 0 and not request_stop:
		print(str(remaining) + " segments remaining")
		print("Fetching segments...")
		sql = "SELECT part FROM cellpath_parts WHERE NOT EXISTS(SELECT * FROM waypoints WHERE waypoints.part = cellpath_parts.part) LIMIT 10000"
		mcur.execute(sql)

		segments = []
		for segment, in mcur.fetchall():
			segments.append(tuple(segment))

		if request_stop:
			break

		print("Calculating waypoints...")
		mapper = util.ParMap(best_waypoint, initializer = init)
		mapper(segments)
		mapper.stop()
		mapper = None
		if request_stop:
			break

		#get number of remaining segments to calculate
		mcur.execute(sql_remaining)
		remaining = mcur.fetchone()[0]

