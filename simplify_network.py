import signal, sys
import psycopg2 #for postgres DB access

import util, config #local modules

def simplify(cell):
	conn = util.db_connect()
	cur = conn.cursor()
	try:
		cur.execute(open("SQL/02_Network_Simplification/simplify_step2.sql", 'r').read(), {"cell": cell})
	except psycopg2.IntegrityError, e:
		if "duplicate key value" in e.pgerror:
			simplify(cell) #try again
		else:
			raise e
	finally:
		conn.commit()

def signal_handler(signal, frame):
	global mapper, request_stop
	if mapper:
		mapper.stop()
	request_stop = True
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

	print("Creating intersection table hh_2po_4pgr_vertices...")
	cur.execute(open("SQL/02_Network_Simplification/create_hh_2po_4pgr_vertices.sql", 'r').read())
	conn.commit()

	print("Creating views for border links/junctions...")
	cur.execute(open("SQL/02_Network_Simplification/borders.sql", 'r').read())
	conn.commit()

	print("Simplifing network (step 1/2)...")
	cur.execute(open("SQL/02_Network_Simplification/simplify_step1.sql", 'r').read())
	conn.commit()

	print("Simplifing network (step 2/2)...")
	mapper = util.ParMap(simplify)
	mapper(config.CELLS, chunksize = 1)
