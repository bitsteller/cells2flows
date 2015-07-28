import psycopg2, signal, sys, os

import util, config #local modules

"""
Compares estimated routes to original MatSim routes to validate the route estimation.
Prerequisites: 
	* run load_matsim.py 
	* import matsim shapefile containing MATSim road network
	* run all steps in run_experiment.py
"""

SAMPLESIZE = 200

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def compare_routes(userid):
	"""Compares the estimated route from SSTEM data to the original MATSim route
	Args:
		userid: the userid for which the comparison should be done
	Returns:
		similarity (0..1) measured by comparing how many points are common (points in intersection of both routes/points in union of both routes)
	"""
	global conn, cur

	#add new cell at centroid of the cluster
	cur.execute("SELECT cmp.similarity, cmp.extra_ms_points, cmp.extra_e_points \
				 FROM compareRoutes((SELECT linkpath FROM matsim WHERE user_id = %(user_id)s), \
									(SELECT route((SELECT trips.cellpath FROM trips WHERE trips.user_id = %(user_id)s))) \
					 			   ) cmp"
				, {"user_id": userid})
	similarity, extra_ms_points, extra_e_points = cur.fetchone()

	if similarity == None:
		print("WARNING: could not compare routes for user " + str(userid))
		return None
	else:
		return {"similarity": similarity, "extra_ms_points": extra_ms_points, "extra_e_points": extra_e_points}

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

def average(values):
	return sum(values)/len(values)

request_stop = False
mapper = None
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	util.db_login()
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating route functions...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_route_functions.sql", 'r').read())
	mconn.commit()

	init_sql_filename = "SQL/04_Routing_Network_Loading/algorithms/" + config.ROUTE_ALGORITHM.upper() + "/init.sql"
	if os.path.exists(init_sql_filename):
		print("Initializing algorithm " + config.ROUTE_ALGORITHM + " (may take a while)...")
		mcur.execute(open(init_sql_filename, 'r').read())
		mconn.commit()

	print("Creating compareRoutes() function...")
	mcur.execute(open("SQL/05_Matsim_Compare/compare_routes.sql", 'r').read())
	mconn.commit()

	mcur.execute("SELECT user_id FROM matsim WHERE EXISTS (SELECT * FROM trips WHERE trips.user_id = matsim.user_id) ORDER BY random() LIMIT %s", (SAMPLESIZE,))
	user_id_generator = (user_id for (user_id,) in mcur)

	print("Comparing " + str(SAMPLESIZE) + " randomly selected routes...")
	mapper = util.ParMap(compare_routes, initializer = init)
	results = mapper(user_id_generator, length = SAMPLESIZE)
	results = [r for r in results if r != None] #trip null values

	print("Average similarity: " + str(average([r["similarity"] for r in results])))
	print("Average extra points in MATSim route: " + str(average([r["extra_ms_points"] for r in results])))
	print("Average extra points in estimated route: " + str(average([r["extra_e_points"] for r in results])))

