import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

#create backup copy ant_pos_full that keeps all antennas even when ant_pos is clustered
print("Creating backup table ant_pos_original (takes a while)...")
cur.execute("DROP TABLE IF EXISTS ant_pos_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE ant_pos_original (LIKE ant_pos);")
cur.execute("ALTER TABLE ant_pos_original ADD CONSTRAINT ant_pos_original_pkey PRIMARY KEY (id);")
cur.execute("INSERT INTO ant_pos_original SELECT * FROM ant_pos;")
conn.commit()



#create backup copy trips_original before clustering
print("Creating backup table trips_orignal (takes a while)...")
cur.execute("DROP TABLE IF EXISTS trips_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE trips_original (LIKE trips);")
cur.execute("ALTER TABLE trips_original ADD CONSTRAINT trips_original_pkey PRIMARY KEY (id);")
cur.execute("INSERT INTO trips_original SELECT * FROM trips;")
conn.commit()