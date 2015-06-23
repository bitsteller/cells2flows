import time, signal, json, random, itertools, math, sys, StringIO
from multiprocessing import Pool, Manager
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access
import numpy #for vector/matrix computations

import util, config #local modules


def calculate_flows(args):
	"""Returns link flows generted by trips from cell origin to all other cells at a given hour"""
	global hour
	o, d = args #arguments are passed as tuple due to pool.map() limitations
	start = time.time()

	conn = util.db_connect()
	cur = conn.cursor()

	#fetch all OD flows from origin
	result = []
	flows_sql = "SELECT cellpath_dist.share * od.flow AS flow, \
						(route_with_waypoints(array_append(array_prepend(best_startpoint(cellpath_dist.cellpath), get_waypoints(cellpath_dist.cellpath)), best_endpoint(cellpath_dist.cellpath)))).edges AS links \
				 FROM cellpath_dist, od\
				 WHERE cellpath_dist.orig_cell = od.orig_cell \
				 AND cellpath_dist.dest_cell = od.dest_cell \
				 AND ((%(interval)s IS NULL AND od.interval IS NULL) OR (od.interval = %(interval)s)) \
				 AND cellpath_dist.orig_cell IN %(orig_cells)s \
				 AND cellpath_dist.dest_cell IN %(dest_cells)s"

	cur.execute(flows_sql, {"interval": hour, "orig_cells": tuple(o), "dest_cells": tuple(d)})
	for flow, links in cur.fetchall():
		result.extend([(link, flow) for link in links])

	end = time.time()
	conn.close()
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

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating network_loading table and loaded_links view...")
	cur.execute(open("SQL/04_Routing_Network_Loading/create_network_loading.sql", 'r').read())
	conn.commit()

	print("Creating route functions...")
	cur.execute(open("SQL/04_Routing_Network_Loading/create_route_functions.sql", 'r').read())
	conn.commit()


	#fetch different interval values
	cur.execute("SELECT array_agg(DISTINCT interval) FROM od")
	intervals = cur.fetchone()[0]
	for i, interval in enumerate(intervals):
		hour = interval
		if request_stop:
			break
		print("Calculating link flows for interval " + str(interval) + " (" + str(i+1) + "/" + str(len(intervals)) + ")...")

		mapper = util.MapReduce(calculate_flows, add_flows) #add flows 
		linkflows = mapper(util.od_chunks(chunksize = 2), length = len(config.CELLS)*len(config.CELLS)//2, chunksize = 1)

		print("Uploading to database...")
		f = StringIO.StringIO("\n".join(["%i\t%i\t%f" % (linkid, interval, flow) for linkid, flow in linkflows]))

		cur.copy_from(f, 'network_loading', columns=('id', 'interval', 'flow'), null = "NULL")
		conn.commit()
