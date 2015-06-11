import time, signal, json, sys, StringIO
import psycopg2 #for postgres DB access
import cPickle as pickle #for loading OD data

import util, config #local modules

def upload_taz(feature):
	"""Uploads a TAZ polygon to the database.
	Args:
		feature: a geojson feature dict containing a TAZ polygon
	"""

	conn = util.db_connect()
	cur = conn.cursor()

	taz_id, linestr = util.parse_taz(feature)

	sql = "	INSERT INTO taz (taz_id, geom) \
			SELECT %(taz_id)s, ST_SetSRID(ST_MakePolygon(ST_GeomFromText(%(linestr)s)),4326);"
	cur.execute(sql, {"taz_id": taz_id, "linestr": linestr})
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

	print("Creating TAZ table...")
	cur.execute(open("SQL/01_Loading/create_taz.sql", 'r').read())
	conn.commit()

	print("Loading TAZ data...")
	taz_json = json.load(open(config.TAZ_FILE, 'r'))

	print("Uploading TAZ data...")
	mapper = util.ParMap(upload_taz, num_workers = 4) 
	tazs = mapper(taz_json["features"])
	tazs = []
	mapper = None

	print("Creating OD table...")
	cur.execute(open("SQL/01_Loading/create_od.sql", 'r').read())
	conn.commit()

	print("Loading OD data...")
	od_pkl = pickle.load(open(config.OD_FILE, 'r'))

	print("Parsing OD data...")
	rows = ["%i\t%i\t%i" % (int(float(ostr)), int(float(dstr)), flow) for (ostr, dstr), flow in od_pkl['0'].items()]
	f = StringIO.StringIO("\n".join(rows))

	print("Upload OD data to database (takes a while)...")
	cur.copy_from(f, 'od', columns=('origin_taz', 'destination_taz', 'flow'))
	conn.commit()

