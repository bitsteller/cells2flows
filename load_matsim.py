import time, signal, json, random, itertools, math, sys
import psycopg2 #for postgres DB access
import numpy as np
import scipy
import matplotlib.pyplot as plt

import config, util

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def read_trip(args):
	"""Parses a line of a matsim route csv
	Args:
		args: a tuple (i, line), where i is the line number and line the a one-line string from the trip csv
	Returns:
		A list [(user_id, cellpath))] where cellpath is a ordered list of cell ids visited on the trip"""

	i, line = args

	t = util.parse_trip(line)
	if not t == None:
		user_id, linkpath = t 
		return [(user_id, linkpath)]
	else:
		return []

def upload_trip(args):
	"""Uploads an antenna position to the database.
	Args:
		args: a tuple ((user_id, cellpath), values), where values is a list that is ignored and the other variables 
		are the attributes of the trip
	"""
	global conn, cur

	user_id, linkpathlist = args
	linkpath = linkpathlist[0]

	sql = "	INSERT INTO matsim (user_id, linkpath) \
			VALUES (%s, %s);"
	cur.execute(sql, (user_id, linkpath))
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

	print("Creating matsim table...")
	mcur.execute(open("SQL/create_matsim.sql", 'r').read())
	mconn.commit()

	#Read trips from file
	#Count lines for status indicator
	linecount = 0
	for line in open(config.MATSIM_FILE).xreadlines(): 
		linecount += 1

	#parse trips
	print("Loading matsim data...")
	mapper = util.MapReduce(read_trip, upload_trip, num_workers = 4, initializer = init)
	trips = mapper(enumerate(open(config.MATSIM_FILE, 'r').xreadlines()), length = linecount, pipe = True, chunksize = 1000, out = False)

	print("Creating matsim_count table...")
	mcur.execute(open("SQL/05_Matsim_Compare/create_matsim_count.sql", 'r').read())
	mconn.commit()	