import datetime, signal, re
import psycopg2 #for postgres DB access

import util, config #local modules

def user_positions(args):
	line, hours = args #pass args as tuple due to paralell pool limitations

	userid, trajectory = re.match(r"([0-9]+)[\s]+\[([\S\s]+)\]",line).groups()
	sequence = re.findall(r"\[\[([\-0-9.]+), ([\-0-9.]+)\], '([0-9:\- ]+)'\]",trajectory) #list of tuples containing userid, lat, lon, time

	result = []
	for lat, lon, timestr in sequence:
		t = datetime.datetime.strptime(timestr.rpartition("-")[0], "%Y-%m-%d %H:%M:%S")
		if t.hour in hours:
			result.append(((userid, float(lat), float(lon)), 1))

	return result

def add(item):
	key, value = item
	return (key, sum(value))

def user_hits(args):
	key, hits = args
	userid, lat, lon = key
	return [(userid, (lat, lon, hits))]

def max_hits(item):
	userid, values = item
	max_hits = max([v[2] for v in values])

	for lat, lon, hits in values:
		if hits == max_hits:
			return (userid, (lat,lon,hits))

def signal_handler(signal, frame):
	global count_cells, request_stop
	if count_cells:
		count_cells.stop()
	request_stop = True
	print("Aborting (can take a minute)...")

request_stop = False
count_cells = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	filename = "/Users/nils/Documents/Studium/Exjobb/ATT/stem_LA_sample_users_all_type_6_0902.csv"
	count_cells = util.MapReduce(user_positions, add)

	print("Reading file and counting cell hits during nighttime...")
	args = ((line, [23,0,1,2,3,4,5]) for line in open(filename, 'r').readlines()) #hours 1-5
	home_cell_counts = count_cells(args, length = 100)

	print("Selecting home cells...")
	select_homes = util.MapReduce(user_hits, max_hits)
	homes = select_homes(home_cell_counts)
	select_homes.stop()


	print("Reading file and counting cell hits during daytime...")
	args = ((line, [8,9,10,11,12,13,14,15,16,17]) for line in open(filename, 'r').readlines()) #hours 1-5
	work_cell_counts = count_cells(args, length = 100)
	count_cells.stop()

	print("Selecting work cells...")
	select_works = util.MapReduce(user_hits, max_hits)
	workplaces = select_works(work_cell_counts)
	select_works.stop()

