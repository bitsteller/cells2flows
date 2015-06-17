import psycopg2, signal, sys

import util, config #local modules

def connected_components(graph):
	"""Returns the vertices in each connected compontent of a given graph
	Args:
		graph: a graph given as a vertex-adjacency dictonary, keys are vertex ids and the value the list of their respective neighbors
	Return:
		A list of lists containing the vertex ids in each connected component
	"""
	components = []
	visited = set()

	def dfs(node):
		if node in visited:
			return []
		visited.add(node)
		nodes = [node]
		for sibling in graph[node]:
			nodes += dfs(sibling)
		return nodes

	for node in graph:
		if node not in visited:
			components.append(dfs(node))
	return components 

def join_cells(args):
	"""Joins all given antennas to one new antenna placed at the centroid of the original antennas.
	Args:
		args: A tuple (newid, cellids) where newid is the new cellid of 
			  the clustered antenna and cellids a list of antenna ids from ant_pos_original to be clustered
	"""
	newid, cellids = args

	conn = util.db_connect()
	cur = conn.cursor()

	#add new cell at centroid of the cluster
	cur.execute("WITH clustered_antennas AS (SELECT ST_Union(ant_pos_original.geom) AS geom FROM ant_pos_original WHERE ant_pos_original.id IN %(cluster)s) \
				 INSERT INTO ant_pos (id, lon, lat, geom) \
				 SELECT %(id)s AS id, \
				 		ST_X(ST_Centroid(clustered_antennas.geom)) AS lon, \
				 		ST_Y(ST_Centroid(clustered_antennas.geom)) AS lat, \
				 		ST_Centroid(clustered_antennas.geom) AS geom \
				 FROM clustered_antennas", {"cluster": tuple(cellids), "id": newid})
	conn.commit()

def update_trip(tripid):
	conn = util.db_connect()
	cur = conn.cursor()

	#fetch cellpath
	cur.execute("SELECT cellpath FROM trips_original WHERE id = %s", (tripid,))
	oldcellpath = cur.fetchone()[0]

	#update antenna ids
	newcellpath = [newcells[oldcell] for oldcell in oldcellpath]

	#remove duplicates #TODO!

	#copy old trip and update cellpath
	cur.execute("INSERT INTO trips SELECT * FROM trips_original WHERE id = %s", (tripid,))
	data = (newcellpath[0], newcellpath[-1], newcellpath, tripid)
	cur.execute("UPDATE trips SET (start_antenna, end_antenna, cellpath) = (%s, %s, %s) WHERE id = %s", data)
	conn.commit()

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
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Recreating ant_pos table...")
	cur.execute(open("SQL/01_Loading/create_ant_pos.sql", 'r').read())
	conn.commit()

	print("Recreating trips table...")
	cur.execute(open("SQL/01_Loading/create_trips.sql", 'r').read())
	conn.commit()

	print("Fetching antennas to join by distance from database...") #All antennas closer than config.MIN_ANTENNA_DIST will be merged
	sql = '''
	SELECT w.id,
	ARRAY(SELECT id
		FROM ant_pos_original
		WHERE ST_DWithin(ST_Transform(geom, 3857), ST_Transform(w.geom, 3857), %(min_dist)s) AND 
		id != w.id
		)
	FROM ant_pos_original AS w;
	'''
	cur.execute(sql, {'min_dist': config.MIN_ANTENNA_DIST})
	graph = {node: edges for node, edges in cur}

	print("Clustering...")
	components = connected_components(graph)

	newcells = dict()
	for newid, oldscells in enumerate(components):
		for oldid in oldscells:
			newcells[oldid] = newid

	print("Updateting antennas...")
	mapper = util.ParMap(join_cells, num_workers = 1) #not parallizable due to necessary write access to cellpath arrays, ParMap just for status indicator
	mapper(enumerate(components), chunksize = 5, length = len(components))

	print("Updateting trips...")
	mapper = util.ParMap(update_trip) #not parallizable due to necessary write access to cellpath arrays, ParMap just for status indicator
	mapper(config.TRIPS, chunksize = 1000)
