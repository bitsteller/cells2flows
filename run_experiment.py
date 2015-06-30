import os, sys, time, subprocess

import util, config

preparations = [ "install and init a postgres database",
				 "install postgis and pgrouting for the database",
				 "install, prepare and start OSRM",
				 "load the road network into the DB using osm2po"
			  ]
steps = [
			("Loading antenna data", "load_antennas.py"),
			("Loading trip data", "load_trips.py"),
			("Loading TAZ and OD data", "load_od.py"),
			("Backup original antenna and trip data", "backup_original.py"),
			("Cluster antennas and preprocess trips", "cluster_antennas.py"), #comment out, if you want to keep original antennas
			("Calculate Voronoi diagram", "voronoi.py"),
			("Simplify road network", "simplify_network.py"),
			("Convert OD matrix to cell matrix", "convert_od.py"),
			("Segmentize cellpaths (for lazy Voronoi routing)", "segmentize_cellpaths.py"),
			("Calculate start/endpoints", "best_points.py"),
			("Calculate waypoints", "waypoints.py"),
			("Calculate cellpath distribution", "cellpath_dist.py"),
			("Loading network", "network_loading.py")
		]


def message(text):
	print(text)
	if hasattr(config, "NOTIFY_CMD"):
		try:
			p = subprocess.Popen(config.NOTIFY_CMD, stdin=subprocess.PIPE, shell=True)
			p.communicate(text + "\n")
		except Exception, e:
			print("Notify command could not be executed: " + e.message)

start_step = 0
if util.confirm("Did you already run parts of the procedure before?", allow_empty = True, default = False):
	print("\n".join(["[" + str(i+1) + "] " + action for i, (action, script) in enumerate(steps)]))
	start_step = int(raw_input("Which step do you want to start from (all previous steps have to be run successfully before and config.py should not have been changed in the meantime)? [1-" + str(len(steps)) + "]: ")) - 1
else:
	for action in preparations:
		if not util.confirm("Did you " + action + "?", allow_empty = True, default = True):
			print("Please " + action + " before you continue.")
			sys.exit()

for i, (action, script) in enumerate(steps[start_step:]):
	print("\033[93m[" + str(i+1) + "/" + str(len(steps)-start_step) + "]\033[0m " + action)
	start = time.time()
	if os.system("python " + script) == 0:
		end = time.time()
		message(action + " finished successfully after " +  str((end-start)/60.0) + " minutes.")
	else:
		message("\033[91m" + action + " exited with errors.\033[0m The rest of the pipeline cannot be executed before this step finished successfully.")
		start_step = start_step + i
		if start_step > 1:
			message("You can start the procedure from step " + str(start_step + 1) + " next time in order to skip the previous steps that finished successfully.")
		sys.exit()

message("Computation done. Find your experiment results in the database.")

