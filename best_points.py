import time, signal, json, random, sys
from multiprocessing import Pool
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def fetch_parts(sql):
	"""Extract unique parts from the database using the given sql query and returns them via a generator.
	Returns:
		A generator object yielding cellpath parts.
	"""
	global mconn, mcur

	mcur.execute(sql)
	for segment, in mcur.fetchall():
		yield tuple(segment)

def best_point(part):
	"""Finds the best start or endpoint for the list of two cells (first two cells in celpath for startpoint, last two for endpoint)
	When the gloabl variable direction = 1 startpoints are calculated, when direction = -1 endpoints are calculated.
	Args:
		part: a 2-tuple of cell ids in the order of travel"""
	global direction, conn, cur

 	a,b = part
	y = None #when no start or endpoint, no border points or no routes were found, null value will be added to the database
	assert direction == 1 or direction == -1

	#find start/end nodes
	data = (a,b) if direction == 1 else (b,a)

	sql = "	SELECT xid, ST_Y(x.geom) AS xlat, ST_X(x.geom) AS xlon\
			FROM closest_junction(%s, %s) AS xid, hh_2po_4pgr_vertices AS x\
			WHERE x.id = xid"
	cur.execute(sql, data)

	if cur.rowcount > 0: #start and end point found
		x, xlat, xlon = cur.fetchone()

		#fetch waypoint candidates
		data = (a,) if direction == 1 else (b,)
		sql = "	SELECT junction_id AS yid, ST_Y(y.geom) AS ylat, ST_X(y.geom) AS ylon\
				FROM boundary_junctions, hh_2po_4pgr_vertices AS y\
				WHERE antenna_id = %s AND y.id = boundary_junctions.junction_id"
		cur.execute(sql, data)
		y_candidates = cur.fetchall()

		#calculate route cost for all waypoints
		costs = []
		for y, ylat, ylon in y_candidates:
			if direction == 1:
				costs.append(route_cost(ylat, ylon, xlat, xlon))
			elif direction == -1:
				costs.append(route_cost(xlat, xlon, ylat, ylon))

		#select cheapest waypoint
		if len(costs) > 0 and min(costs) < float("inf"): #at least one feasible route found
			y = y_candidates[costs.index(min(costs))][0]

	data = ([a,b], y)
	if direction == 1:
		cur.execute("INSERT INTO best_startpoint VALUES (%s,%s);", data)

	elif direction == -1:
		cur.execute("INSERT INTO best_endpoint VALUES (%s,%s);", data)
	conn.commit()

def route_cost(xlat, xlon, ylat, ylon, attempts = 3):
	"""Calculates the cost from x via y to z; OSRM backend needs to listen at port 5000
	Args:
		xlat: latitude of the start point
		xlon: longitude of the start point
		ylat: latitude of the via point
		ylon: longitude of the via point
	Returns:
		The cost of the calculated route (travel time in seconds) or inf if no route found"""

	data = {}
	try:
		data = json.load(urllib2.urlopen('http://www.server.com:5000/viaroute?loc=' + str(xlat) + ',' + str(xlon) + '&loc=' + str(ylat) + ',' + str(ylon)))
	except Exception, e:
		print("WARNING: " + str(e))
		if attempts > 0:
			time.sleep(5)
			route_cost(xlat, xlon, ylat, ylon, attempts = attempts - 1)
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

	print("Creating best point tables...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_best_points.sql", 'r').read())
	mconn.commit()

	print("Creating closest_junction() function...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_closest_junction_func.sql", 'r').read())
	mconn.commit()

	#get number of remaining segments to calculate
	sql_remaining = "SELECT COUNT(DISTINCT trips.cellpath[1:2]) FROM trips WHERE array_length(trips.cellpath, 1) >= 2 AND NOT EXISTS(SELECT * FROM best_startpoint WHERE best_startpoint.part = trips.cellpath[1:2])"
	mcur.execute(sql_remaining)
	remaining = mcur.fetchone()[0]

	startparts = fetch_parts("SELECT DISTINCT trips.cellpath[1:2] FROM trips WHERE array_length(trips.cellpath, 1) >= 2 AND NOT EXISTS(SELECT * FROM best_startpoint WHERE best_startpoint.part = trips.cellpath[1:2])")

	print("Calculating startpoints...")
	direction = 1 #startpoints
	mapper = util.ParMap(best_point, initializer = init)
	mapper(startparts, length = remaining)
	mapper.stop()

	#get number of remaining segments to calculate
	sql_remaining = "SELECT COUNT(DISTINCT trips.cellpath[array_upper(trips.cellpath,1)-1:array_upper(trips.cellpath,1)]) FROM trips WHERE array_length(trips.cellpath, 1) >= 2 AND NOT EXISTS(SELECT * FROM best_endpoint WHERE best_endpoint.part = trips.cellpath[array_upper(trips.cellpath,1)-1:array_upper(trips.cellpath,1)])"
	mcur.execute(sql_remaining)
	remaining = mcur.fetchone()[0]

	endparts = fetch_parts("SELECT DISTINCT trips.cellpath[array_upper(trips.cellpath,1)-1:array_upper(trips.cellpath,1)] FROM trips WHERE array_length(trips.cellpath, 1) >= 2 AND NOT EXISTS(SELECT * FROM best_endpoint WHERE best_endpoint.part = trips.cellpath[array_upper(trips.cellpath,1)-1:array_upper(trips.cellpath,1)])")

	print("Calculating endpoints...")
	direction = -1 #endpoints
	mapper = util.ParMap(best_point, initializer = init)
	mapper(endparts, length = remaining)
	mapper.stop()

