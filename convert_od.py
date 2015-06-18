import psycopg2, signal, sys

import util, config #local modules

def fetch_taz_od():
	conn = util.db_connect()
	cur = conn.cursor()
	cur.execute("SELECT orig_taz, dest_taz, flow  \
			 FROM taz_od \
			 WHERE ((interval IS NULL AND %(interval)s IS NULL) OR interval = %(interval)s)",
			 {"interval": interval})

	for o_taz, d_taz, flow in cur:
		yield (o_taz, d_taz, flow)

def calculate_cell_od_flow(args):
	global interval
	o_taz, d_taz, flow = args

	#distribute flow on cells by area
	#TODO

def upload_cell_od_flow(args):
	global interval
	key, flows = args
	o_cell, d_cell = key
	flow = sum(flows)
	
	data = {"orig_cell": o_cell,
			"dest_cell": d_cell,
			"interval": interval,
			"flow": flow}

	cur.execute(open("SQL/01a_Preprocessing/add_cell_od_flow.sql", 'r').read(), data)
	conn.execute()

def signal_handler(signal, frame):
	global mapper, request_stop
	if mapper:
		mapper.stop()
	request_stop = True
	print("Aborting (can take a minute)...")

request_stop = False
mapper = None
interval = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	util.db_login()
	conn = util.db_connect()
	cur = conn.cursor()

	print("Creating od table...")
	cur.execute(open("SQL/01a_Preprocessing/create_od.sql", 'r').read())
	conn.commit()


	#fetch different interval values
	cur.execute("SELECT array_agg(DISTINCT interval) FROM taz_od")
	intervals = cur.fetchone()[0]
	for i, interval in enumerate(intervals):
		print("Converting OD flows to cellflows for interval " + str(interval) + " (" + str(i+1) + "/" + str(len(intervals)) + ")...")

		#count OD pairs in TAZ OD for given interval
		cur.execute("SELECT COUNT(*) \
					 FROM taz_od \
					 WHERE ((interval IS NULL AND %(interval)s IS NULL) OR interval = %(interval)s)",
					 {"interval": interval})
		count = cur.fetchone()[0]

		#convert to cell flows
		mapper = util.MapReduce(calculate_cell_od_flow, upload_cell_od_flow)
		mapper(fetch_taz_od(), pipe = True, out = False, chunksize = 1000)


