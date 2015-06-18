import psycopg2, signal, sys

import util, config #local modules

def calculate_cell_od_flow(args):
	global interval
	o_taz, d_taz, flow = args

	#distribute flow on cells by area
	pass

def upload_cell_od_flow(args):
	global interval
	key, flows = args
	o_cell, d_cell = key
	flow = sum(flows)
	


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

