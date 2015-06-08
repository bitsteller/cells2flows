import time, signal, json, random, itertools, math, sys
import psycopg2 #for postgres DB access
import numpy as np
import scipy
import matplotlib.pyplot as plt

import util, config #local modules

def read_trip(args):
	"""Parses a line of a trip csv
	Args:
		args: a tuple (i, line), where i is the line number and line the a one-line string from the trip csv
	Returns:
		A list [(user_id, cellpath))] where cellpath is a ordered list of cell ids visited on the trip"""

	i, line = args

	t = util.parse_trip(line)
	if not t == None:
		user_id, cellpath = t 
		return [(user_id, cellpath)]
	else:
		return []

def upload_trip(args):
	"""Uploads an antenna position to the database.
	Args:
		args: a tuple ((user_id, cellpath), values), where values is a list that is ignored and the other variables 
		are the attributes of the trip
	"""

	user_id, cellpathlist = args
	cellpath = cellpathlist[0]
	conn = util.db_connect()
	cur = conn.cursor()
	sql = "	INSERT INTO trips (user_id, start_antenna, end_antenna, cellpath) \
			VALUES (%s, %s, %s, %s);"
	cur.execute(sql, (user_id, cellpath[0], cellpath[-1], cellpath))
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
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating trips table...")
	cur.execute(open("SQL/01_Loading/create_trips.sql", 'r').read())
	conn.commit()

	#Read trips from file
	#Count lines for status indicator
	linecount = 0
	for line in open(config.TRIPS_FILE).xreadlines(): 
		linecount += 1

	#parse trips
	print("Loading trips...")
	mapper = util.MapReduce(read_trip, upload_trip, num_workers = 4)
	trips = mapper(enumerate(open(config.TRIPS_FILE, 'r').readlines()), length = linecount, pipe = True)
	conn.commit()