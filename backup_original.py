import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

#create backup copy ant_pos_full that keeps all antennas even when ant_pos is clustered
print("Creating backup table ant_pos_original (takes a while)...")
cur.execute("DROP TABLE IF EXISTS ant_pos_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE ant_pos_original AS SELECT * FROM ant_pos;")
conn.commit()

#create backup copy trips_original before clustering
print("Creating backup table trips_orignal (takes a while)...")
cur.execute("DROP TABLE IF EXISTS trips_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE trips_original AS SELECT * FROM trips;")
conn.commit()

#create backup copy taz_original before joining TAZs
print("Creating backup table taz_original (takes a while)...")
cur.execute("DROP TABLE IF EXISTS taz_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE taz_original AS SELECT * FROM taz;")
conn.commit()

#create backup copy od_original before joining TAZs
print("Creating backup table od_original (takes a while)...")
cur.execute("DROP TABLE IF EXISTS od_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE od_original AS SELECT * FROM od;")
conn.commit()