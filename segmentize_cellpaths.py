import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

print("Creating simple_cellpath view (takes a while)...")
cur.execute(open("SQL/04_Routing_Network_Loading/create_simple_cellpath.sql", 'r').read(), {"tolerance": config.SIMPLIFICATION_TOLERANCE})
conn.commit()

print("Creating cellpath_segment view (takes a while)...")
cur.execute(open("SQL/04_Routing_Network_Loading/create_cellpath_segment.sql", 'r').read())
conn.commit()