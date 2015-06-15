import psycopg2
import util, config

MIN_DIST = 150 # Distance between waypoints in meters

SQL = '''
SELECT w.id,
  ARRAY(SELECT id
		FROM ant_pos
		WHERE ST_DWithin(ST_Transform(geom, 3857), ST_Transform(w.geom, 3857), %(min_dist)s) AND 
			  id != w.id
  )
FROM ant_pos AS w;
'''

def connected_components(graph):
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

if __name__ == '__main__':
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Fetching antennas to join by distance from database...")
	cur.execute(SQL, {'min_dist': MIN_DIST})
	graph = {node: edges for node, edges in cur}

	print("Clustering...")
	components = connected_components(graph)

	print("Uploading clusters to the database...")
	for label, cells in enumerate(components):
		cur.executemany("UPDATE ant_pos SET cluster = %s WHERE id = %s;", [(label, cellid) for cellid in cells])
		conn.commit()

