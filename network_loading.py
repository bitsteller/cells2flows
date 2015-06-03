import time, signal, json, random, itertools, math, sys
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
	flows_sql = "SELECT cellpath_dist.share * dyn_od.trip_weight AS flow, \
						(route_with_waypoints(array_append(array_prepend(best_startpoint(cellpath_dist.cellpath), get_waypoints(cellpath_dist.cellpath)),best_endpoint(cellpath_dist.cellpath)))).edges AS links \
				 FROM cellpath_dist, od AS dyn_od \
				 WHERE cellpath_dist.start_antenna = dyn_od.start_antenna \
				 AND cellpath_dist.end_antenna = dyn_od.end_antenna \
				 AND dyn_od.time_interval = %s \
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
	if mapper:
		mapper.stop()
	request_stop = True
	print("Aborting (can take a minute)...")

request_stop = False
mapper = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()

	print("Deleting from network_loading table...")
	conn = util.db_connect()
	cur = conn.cursor()
	cur.execute("DELETE FROM network_loading")
	conn.commit()

	#calculate link flows
	mapper = util.MapReduce(calculate_flows, add_flows, num_workers = 4) #add flows 
	for hour in range(0,24):
		if request_stop:
			break

		print("Calculating link flows for interval " + str(hour) + "...")
		args = []
		for od in util.od_chunks():
			args.append((hour, od))
		linkflows = mapper(args)
		print("Uploading to database...")

	#calculate_od((1,5,timedist)) #test
