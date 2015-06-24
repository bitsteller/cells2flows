import signal, sys
import psycopg2 #for postgres DB access

import util, config #local modules

def fetch_cellpaths(chunksize = 100):
	conn = util.db_connect()
	cur = conn.cursor()
	cur.execute("SELECT DISTINCT cellpath FROM trips")

	batch = []
	for cellpath in cur:
		batch.append(cellpath)
		if len(batch) >= 100:
			yield batch
			batch = []
	if len(batch) > 0:
		yield batch

def simplify(cellpaths):
	conn = util.db_connect()
	cur = conn.cursor()

	conn.commit()

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

request_stop = False
mapper = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating cellpath_segments table...")
	cur.execute(open("SQL/04_Routing_Network_Loading/create_cellpath_segments.sql", 'r').read())
	conn.commit()

	print("Simplifying cellpaths...")
	cur.execute("SELECT COUNT(DISTINCT cellpath) FROM trips")
	count = cur.fetchone()[0]

	mapper = util.ParMap(simplify)
	mapper(fetch_cellpaths(), length = count, chunksize = 100)
