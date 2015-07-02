import time, signal, json, random, itertools, math, sys
import psycopg2 #for postgres DB access
import numpy as np
import scipy
import matplotlib.pyplot as plt

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

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
	global conn, cur

	key, values = args

	cell, lat, lon, srid = key
	sql = "	INSERT INTO ant_pos (id, lon, lat, geom) \
 			WITH p AS (SELECT ST_Transform(ST_SetSRID(ST_MakePoint(%(lat)s,%(lon)s),%(srid)s), 4326) AS point) \
			SELECT %(cell)s, ST_Y(p.point), ST_X(p.point), p.point FROM p;"
	cur.execute(sql, {"lat": lat, "lon": lon, "srid": srid, "cell": cell})
	conn.commit()

	return None

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
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating antenna table...")
	mcur.execute(open("SQL/01_Loading/create_ant_pos.sql", 'r').read())
	mconn.commit()

	#Read antennas from file
	#Count lines for status indicator
	linecount = 0
	for line in open(config.ANTENNA_FILE).xreadlines(): 
		linecount += 1

	#parse antennas
	print("Loading antenna table...")
	mapper = util.MapReduce(antenna_position, upload_antenna, num_workers = 4, initializer = init)
	antennas = mapper(enumerate(open(config.ANTENNA_FILE, 'r').readlines()), length = linecount, pipe = True, out = False)
