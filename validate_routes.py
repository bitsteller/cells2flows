import psycopg2, signal, sys, os

import numpy as np
import matplotlib.pyplot as plt

import util, config #local modules

"""
Compares estimated routes to original MatSim routes to validate the route estimation.
Prerequisites: 
	* run load_matsim.py 
	* import matsim shapefile containing MATSim road network
	* run all steps in run_experiment.py
"""

SAMPLESIZE = 1000

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

def validate_agorithm(algo):
	init_sql_filename = "SQL/04_Routing_Network_Loading/algorithms/" + algo + "/init.sql"
	if os.path.exists(init_sql_filename):
		print("Initializing algorithm " + algo + " (may take a while)...")
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
	mapper.stop()
	results = [r for r in results if r != None] #trip null values

	mean = average([r["similarity"] for r in results])
	std = np.std(np.array([r["similarity"] for r in results]))

	print("Average similarity: " + str(mean) + " (std: " + str(std) + ")")
	print("Average extra points in MATSim route: " + str(average([r["extra_ms_points"] for r in results])))
	print("Average extra points in estimated route: " + str(average([r["extra_e_points"] for r in results])))

	return mean, std

def plot_results(means, stds, labels):
	ind = np.arange(len(labels))  # the x locations for the groups
	width = 0.5       # the width of the bars

	fig, ax = plt.subplots()
	rects1 = ax.bar(ind + 0.25, means, width, color='r', yerr=stds)

	# add some text for labels, title and axes ticks
	ax.set_ylabel('Route recoverage')
	ax.set_title('Similarity of estimated routes to MATSim routes')
	ax.set_xticks(ind+width)
	ax.set_xticklabels( tuple(labels) )
	
	plt.ylim(ymin = 0.0, ymax = 1.0)

	def autolabel(rects):
	    # attach some text labels
	    for rect in rects:
	        height = rect.get_height()
	        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, str("{:10.2f}".format(height)),
	                ha='center', va='bottom')

	autolabel(rects1)
	plt.show()

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	util.db_login()
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating route functions...")
	mcur.execute(open("SQL/04_Routing_Network_Loading/create_route_functions.sql", 'r').read())
	mconn.commit()

	means = []
	stds = []
	labels = []
	for algo in ["shortest", "strict", "lazy"]:
		mean, std = validate_agorithm(algo)
		means.append(mean)
		stds.append(std)
		labels.append(algo)

	plot_results(means, stds, labels)

