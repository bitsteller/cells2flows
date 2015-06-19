import psycopg2, signal, sys, time

import util, config #local modules

CHUNKSIZE = 100

def fetch_taz_od_chunks():
	"""Fetches chunks of rows from the taz_od table of the database
	Returns:
		a generator returning lists containing row tuples (origin_taz, destination_taz, flow) fetched from the database
	"""

	conn = util.db_connect()
	cur = conn.cursor()
	cur.execute("SELECT origin_taz, destination_taz, flow  \
			 FROM taz_od \
			 WHERE ((interval IS NULL AND %(interval)s IS NULL) OR interval = %(interval)s)",
			 {"interval": interval})

	batch = []
	for o_taz, d_taz, flow in cur:
		batch.append((o_taz, d_taz, flow))
		if len(batch) == CHUNKSIZE:
			yield batch
			batch = []
	yield batch

def calculate_cell_od_flow(taz_od_chunk):
	"""Converts TAZ OD to cell OD flows, where the cell OD flow is calculated 
	based on the share of the area that the origin and destination cells cover.
	Args:
		taz_od_chunk: a list of tuples (origin_taz, destination_taz, flow), where the first two are TAZ ids
	Returns:
		A list of tuples ((origin_cell, destination_cell), flow)
	"""
	global interval

	conn = util.db_connect()
	cur = conn.cursor()
	result = []

	#fetch origin/destination cell coverages
	tazs = []
	for o_taz, d_taz, flow in taz_od_chunk:
		tazs.append(o_taz)
		tazs.append(d_taz)

	cur.execute("SELECT taz_id, cell_id, share FROM taz_cells WHERE taz_id IN %s", (tuple(taz),))
	coverages = cur.fetchall()

	for o_taz, d_taz, flow in taz_od_chunk:
		o_cells = []
		d_cells = []
		for taz_id, cell_id, share in coverages:
			if taz_id == o_taz:
				o_cells.append((cell_id, share))
			if taz_id == d_taz:
				d_cells.append((cell_id, share))

		#distribute flow on cells by area
		od_pairs = []
		shares = []
		for o_cell, o_share in o_cells:
			for d_cell, d_share in d_cells:
				if o_share * d_share > 0.1:
					od_pairs.append((o_cell, d_cell))
					shares.append(o_share * d_share)

		normalized_shares = [share/sum(shares) for share in shares]
		result.extend([((o_cell, d_cell), share * flow) for (o_cell, d_cell), share in zip(od_pairs, normalized_shares)])
	return result

def upload_cell_od_flow(args):
	"""Aggregates cell OD flows and adds the flows to the database, 
	by updateing an exisiting cell OD pair row or createing a new one if no row exisits.
	Args:
		args: a tuple (key, flows), where key is a tuple (o_cell, d_cell) containing the originn and desitination cell ids and
			flows is a list of flows that occur on the given OD pair
	"""
	global interval
	key, flows = args
	o_cell, d_cell = key
	flow = sum(flows)

	conn = util.db_connect()
	cur = conn.cursor()
	
	data = {"orig_cell": o_cell,
			"dest_cell": d_cell,
			"interval": interval,
			"flow": flow}

	cur.execute(open("SQL/01a_Preprocessing/add_cell_od_flow.sql", 'r').read(), data)
	conn.commit()

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

	print("Creating taz_cell view mapping TAZ to cells...")
	cur.execute(open("SQL/01a_Preprocessing/create_get_cells_for_taz_func.sql", 'r').read())
	conn.commit()

	#fetch different interval values
	cur.execute("SELECT array_agg(DISTINCT interval) FROM taz_od")
	intervals = cur.fetchone()[0]
	for i, interval in enumerate(intervals):
		print("Converting TAZ OD flows to cell OD flows for interval " + str(interval) + " (" + str(i+1) + "/" + str(len(intervals)) + ")...")

		#count OD pairs in TAZ OD for given interval
		cur.execute("SELECT COUNT(*) \
					 FROM taz_od \
					 WHERE ((interval IS NULL AND %(interval)s IS NULL) OR interval = %(interval)s)",
					 {"interval": interval})
		count = cur.fetchone()[0]

		#convert to cell flows
		mapper = util.MapReduce(calculate_cell_od_flow, upload_cell_od_flow)
		mapper(fetch_taz_od_chunks(), length = count/CHUNKSIZE, pipe = True, out = False)


