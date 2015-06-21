import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

print("Creating cellpath distribution (takes a while)...")
cur.execute(open("SQL/04_Routing_Network_Loading/create_cellpath_dist.sql", 'r').read())
conn.commit()