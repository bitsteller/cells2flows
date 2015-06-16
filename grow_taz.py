import psycopg2, signal, sys

import util, config #local modules

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

	print("Restoring original data from taz_original (takes a while)...")
	cur.execute(open("SQL/01_Loading/create_taz.sql", 'r').read())
	conn.commit()
	cur.execute("INSERT INTO taz SELECT * FROM taz_original;")
	conn.commit()

	print("Restoring original data from od_original (takes a while)...")
	cur.execute(open("SQL/01_Loading/create_od.sql", 'r').read())
	conn.commit()
	cur.execute("INSERT INTO od SELECT * FROM od_original;")
	conn.commit()

