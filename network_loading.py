import time, signal, json, random, itertools, math, sys, StringIO
from multiprocessing import Pool, Manager
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access
import numpy #for vector/matrix computations

import util, config #local modules


def calculate_flows(args):
	"""Returns link flows generted by trips from cell origin to all other cells at a given hour"""

	hour, od = args #arguments are passed as tuple due to pool.map() limitations
	o, d = od
	start = time.time()

	conn = util.db_connect()
	cur = conn.cursor()

	#fetch all OD flows from origin
	result = []
	flows_sql = "SELECT cellpath_dist.share * od.flow AS flow, \
						(route_with_waypoints(array_append(array_prepend(best_startpoint(cellpath_dist.cellpath), get_waypoints(cellpath_dist.cellpath)), best_endpoint(cellpath_dist.cellpath)))).edges AS links \
				 FROM cellpath_dist, od\
				 WHERE cellpath_dist.start_antenna = od.start_antenna \
				 AND cellpath_dist.end_antenna = od.end_antenna \
				 AND od.time_interval = %s \
				 AND cellpath_dist.start_antenna IN %s \
				 AND cellpath_dist.end_antenna IN %s"

	cur.execute(flows_sql, (hour, tuple(o), tuple(d)))

	for flow, links in cur.fetchall():
		result.extend([(link, flow) for link in links])

	end = time.time()
	conn.close()
	return result
	#print(str(o) + "->" + str(d) + " done after " + str(end-start) + "s")

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

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating network_loading table and loaded_links view...")
	cur.execute(open("SQL/04_Routing_Network_Loading/create_network_loading.sql", 'r').read())
	conn.commit()

	print("Creating cellpath distribution (takes a while)...")
	cur.execute(open("SQL/04_Routing_Network_Loading/create_cellpath_dist.sql", 'r').read())
	conn.commit()

	#calculate link flows
	mapper = util.MapReduce(calculate_flows, add_flows) #add flows 
	for hour in range(0,24):
		if request_stop:
			break

		print("Calculating link flows for interval " + str(hour) + "...")
		args = []
		for od in util.od_chunks(chunksize = 10):
			args.append((hour, od))
		linkflows = mapper(args)

		print("Uploading to database...")
		f = StringIO.StringIO("\n".join(["%i\t%f\t%i" % (linkid, flow, hour) for linkid, flow in linkflows]))
		cur.copy_from(f, 'network_loading', columns=('id', 'flow', 'hour'))
		conn.commit()
