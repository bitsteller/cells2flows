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

def join_cells(cellids):
	"""Joins to antennas to one new antenna placed at the centroid of the original antennas. Also updates the cellpaths in all trips accordingly.
	Args:
		cellids: A list cellids to join
	"""
	if len(cellids) < 2:
		return #single antenna in cluster, no join necessary

	conn = util.db_connect()
	cur = conn.cursor()

	#add new cell at centroid of the cluster
	cur.execute("SELECT MAX(id) FROM ant_pos")
	newid = cur.fetchone()[0] + 1
	cur.execute("WITH clustered_antennas AS (SELECT ST_Union(ant_pos.geom) AS geom FROM ant_pos WHERE ant_pos.id IN %(cluster)s) \
				 INSERT INTO ant_pos (id, lon, lat, geom) \
				 SELECT %(id)s AS id, \
				 		ST_X(ST_Centroid(clustered_antennas.geom)) AS lon, \
				 		ST_Y(ST_Centroid(clustered_antennas.geom)) AS lat, \
				 		ST_Centroid(clustered_antennas.geom) AS geom \
				 FROM clustered_antennas", {"cluster": tuple(cellids), "id": newid})

	#replace old cell ids in cellpaths in trip table
	data = [{"oldcell": oldcell, "newcell": newid} for oldcell in cellids]
	cur.executemany(open("SQL/01a_Preprocessing/replace_antenna_in_trips.sql", 'r').read(), data)

	#remove old cells
	cur.execute("DELETE FROM ant_pos WHERE id IN %s", (tuple(cellids),))
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

	print("Creating array_replace function...")
	cur.execute(open("SQL/01a_Preprocessing/create_array_replace_func.sql", 'r').read())
	conn.commit()

	print("Fetching antennas to join by distance from database...") #All antennas closer than config.MIN_ANTENNA_DIST will be merged
	sql = '''
	SELECT w.id,
	ARRAY(SELECT id
		FROM ant_pos
		WHERE ST_DWithin(ST_Transform(geom, 3857), ST_Transform(w.geom, 3857), %(min_dist)s) AND 
		id != w.id
		)
	FROM ant_pos AS w;
	'''
	cur.execute(sql, {'min_dist': config.MIN_ANTENNA_DIST})
	graph = {node: edges for node, edges in cur}

	print("Clustering...")
	components = connected_components(graph)

	print("Updateing antenna and trip tables...")
	mapper = util.ParMap(join_cells, num_workers = 1) #not parallizable due to necessary write access to cellpath arrays, ParMap just for status indicator
	mapper(components, chunksize = 5)


