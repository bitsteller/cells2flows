import psycopg2, signal, sys, time

import util, config #local modules

CHUNKSIZE = 1000

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def fetch_taz_od_chunks():
	"""Fetches chunks of rows from the taz_od table of the database
	Returns:
		a generator returning lists containing row tuples (origin_taz, destination_taz, flow) fetched from the database
	"""
	global mconn, mcur
	
	mcur.execute("SELECT origin_taz, destination_taz, flow  \
			 FROM taz_od \
			 WHERE ((interval IS NULL AND %(interval)s IS NULL) OR interval = %(interval)s)",
			 {"interval": interval})

	batch = []
	for o_taz, d_taz, flow in mcur:
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
	global interval, cur, conn
	result = []

	#fetch origin/destination cell coverages
	tazs = []
	for o_taz, d_taz, flow in taz_od_chunk:
		tazs.append(o_taz)
		tazs.append(d_taz)

	cur.execute("SELECT taz_id, cell_id, share FROM taz_cells WHERE taz_id IN %s", (tuple(tazs),))
	coverages = cur.fetchall()

	for o_taz, d_taz, flow in taz_od_chunk:
		o_cells = []
		d_cells = []
		for taz_id, cell_id, share in coverages:
			if taz_id == o_taz:
				o_cells.append((cell_id, share))
			if taz_id == d_taz:
				d_cells.append((cell_id, share))

		#find best matching cell pairs by area
		od_pairs = []
		for o_cell, o_share in o_cells:
			for d_cell, d_share in d_cells:
				od_pairs.append((o_cell, d_cell, o_share*d_share))

		od_pairs = sorted(od_pairs, key=lambda od_pair: od_pair[2])
		od_pairs.reverse() #sort descending by share

		top_od_pairs = []
		for i in range(len(od_pairs)):
			o_cell, d_cell, combined_share = od_pairs[i]
			if combined_share >= 0.8: #keep all almost fully covered cell pairs
				top_od_pairs.append((o_cell, d_cell, combined_share))
			elif i < 5: #if less than 5 fully covered cell pairs: kepp additionally up to 5 cell pairs that are not fully covered
				top_od_pairs.append((o_cell, d_cell, combined_share))

		if len(top_od_pairs) == 0:
			print("WARNING: Flow of " + str(flow) + " was lost, because no cellpair could be found for TAZ pair (" + str(o_taz), "," + str(d_taz) + ")!")
		
		#allocate flow to the discovered cell pairs
		share_sum = sum([combined_share for o_cell, d_cell, combined_share in top_od_pairs])
		normalized_shares = [(o_cell, d_cell, share/share_sum) for o_cell, d_cell, share in top_od_pairs]
		result.extend([((o_cell, d_cell), share * flow) for o_cell, d_cell, share in normalized_shares])
	return result

def upload_cell_od_flow(args):
	"""Aggregates cell OD flows and adds the flows to the database, 
	by updateing an exisiting cell OD pair row or createing a new one if no row exisits.
	Args:
		args: a tuple (key, flows), where key is a tuple (o_cell, d_cell) containing the originn and desitination cell ids and
			flows is a list of flows that occur on the given OD pair
	"""
	global interval, cur, conn

	key, flows = args
	o_cell, d_cell = key
	flow = sum(flows)
	
	data = {"orig_cell": o_cell,
			"dest_cell": d_cell,
			"interval": interval,
			"flow": flow}

	cur.execute(open("SQL/01a_Preprocessing/add_cell_od_flow.sql", 'r').read(), data)
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
interval = None
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating od table...")
	mcur.execute(open("SQL/01a_Preprocessing/create_od.sql", 'r').read())
	mconn.commit()

	print("Creating taz_cell view mapping TAZ to cells...")
	mcur.execute(open("SQL/01a_Preprocessing/create_get_cells_for_taz_func.sql", 'r').read())
	mconn.commit()

	#fetch different interval values
	for i, interval in enumerate(config.INTERVALS):
		print("Converting TAZ OD flows to cell OD flows for interval " + str(interval) + " (" + str(i+1) + "/" + str(len(config.INTERVALS)) + ")...")

		#count OD pairs in TAZ OD for given interval
		mcur.execute("SELECT COUNT(*) \
					 FROM taz_od \
					 WHERE interval = %(interval)s",
					 {"interval": interval})
		count = mcur.fetchone()[0]

		#convert to cell flows
		mapper = util.MapReduce(calculate_cell_od_flow, upload_cell_od_flow, initializer = init)
		mapper(fetch_taz_od_chunks(), length = count//CHUNKSIZE + 1, pipe = True, out = False, chunksize = 1)