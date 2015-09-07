import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

POPULATION = 13508715 #number of inhabitants in the area covered by all cells


print("Creating cell population view (1/4)...")
cur.execute(open("SQL/03_Scaling_OD/cell_population_view.sql", 'r').read(), {"population": POPULATION})
conn.commit()

print("Creating cell_factors view (2/4)...")
cur.execute(open("SQL/03_Scaling_OD/cell_factors_view.sql", 'r').read(), {"cells": config.CELLS})
conn.commit()

print("Creating user_factors view (3/4)...")
cur.execute(open("SQL/03_Scaling_OD/user_factors_view.sql", 'r').read())
conn.commit()

print("Creating trip_factors view (4/4)...")
cur.execute(open("SQL/03_Scaling_OD/trip_factors_view.sql", 'r').read())
conn.commit()