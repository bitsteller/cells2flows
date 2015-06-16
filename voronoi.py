import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

print("Creating Voronoi table...")
cur.execute(open("SQL/01_Loading/create_voronoi.sql", 'r').read())

print("Creating voronoi() function...")
cur.execute(open("SQL/01_Loading/create_voronoi_func.sql", 'r').read())

print("Calculating Voronoi partition...")
cur.execute("INSERT INTO voronoi SELECT v.id, v.geom FROM voronoi('ant_pos', 'geom') AS v(id numeric(10,0), geom geometry(Polygon,4326))")
conn.commit()
