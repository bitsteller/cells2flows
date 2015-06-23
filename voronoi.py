import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

print("Creating Voronoi table...")
cur.execute(open("SQL/01_Loading/create_voronoi.sql", 'r').read())

print("Creating voronoi() function...")
cur.execute(open("SQL/01_Loading/create_voronoi_func.sql", 'r').read())
conn.commit()

print("Calculating Voronoi partition...")
cur.execute("	DROP TABLE IF EXISTS ant_pos_ordered;\
        		CREATE TEMPORARY TABLE ant_pos_ordered (LIKE ant_pos);\
       			INSERT INTO ant_pos_ordered SELECT * FROM ant_pos wp ORDER BY id ASC;")
cur.execute("INSERT INTO voronoi SELECT v.id, v.geom FROM voronoi('ant_pos_ordered', 'geom') AS v(id numeric(10,0), geom geometry(Polygon,4326))")
conn.commit()
