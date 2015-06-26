import time, signal, json, random, itertools, math, sys, StringIO
from multiprocessing import Pool, Manager
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access
import numpy #for vector/matrix computations

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def calculate_flows(args):
	"""Returns link flows generted by trips from cell origin to all other cells at a given hour"""
	global hour, conn, cur
	o, d = args #arguments are passed as tuple due to pool.map() limitations

	#fetch all OD flows from origin
	result = []
	#t = util.Timer("DB")
	flows_sql = "SELECT cellpath_dist.share * od.flow AS flow, \
						(route_with_waypoints(array_append(array_prepend(best_startpoint(cellpath_dist.cellpath), get_waypoints(cellpath_dist.cellpath)), best_endpoint(cellpath_dist.cellpath)))).edges AS links \
				 FROM cellpath_dist, od\
				 WHERE cellpath_dist.orig_cell = od.orig_cell \
				 AND cellpath_dist.dest_cell = od.dest_cell \
				 AND od.interval = %(interval)s \
				 AND cellpath_dist.orig_cell = ANY(%(orig_cells)s) \
				 AND cellpath_dist.dest_cell = ANY(%(dest_cells)s)"
	cur.execute(flows_sql, {"interval": hour, "orig_cells": o, "dest_cells": d})
	#t.stop()
	for flow, links in cur.fetchall():
		result.extend([(link, flow) for link in links])

	return result

def add_flows(item):
	link, flows = item
	return (link, sum(flows))

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

request_stop = False
mapper = None
hour = None
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating network_loading table and loaded_links view...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_network_loading.sql", 'r').read())
	mconn.commit()

	print("Creating route functions...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_route_functions.sql", 'r').read())
	mconn.commit()

	#fetch different interval values
	for i, interval in enumerate(config.INTERVALS):
		hour = interval
		if request_stop:
			break
		print("Calculating link flows for interval " + str(interval) + " (" + str(i+1) + "/" + str(len(config.INTERVALS)) + ")...")

		mapper = util.MapReduce(calculate_flows, add_flows, initializer = init) #add flows 
		linkflows = mapper(util.od_chunks(chunksize = 3), length = len(config.CELLS)*len(config.CELLS)//3 + len(config.CELLS), chunksize = 3)

		print("Uploading to database...")
		f = StringIO.StringIO("\n".join(["%i\t%i\t%f" % (linkid, interval, flow) for linkid, flow in linkflows]))

		mcur.copy_from(f, 'network_loading', columns=('id', 'interval', 'flow'), null = "NULL")
		mconn.commit()
