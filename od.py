import time, signal, json, random, itertools, math, sys
from multiprocessing import Pool, Manager
import psycopg2 #for postgres DB access
import numpy #for vector/matrix computations

import util, config #local modules

def fetch_timedist():
	"""Fetches the time distribution of trips from the database by counting 
	the number of starting trips in every 10min interval of the day.

	Returns:
		A list of size 24*6 containing the number of trips that start in every 
		10min interval of the day.
	"""

	conn = util.db_connect()
	cur = conn.cursor()

	cur.execute("SELECT EXTRACT(HOUR FROM trips.start_time)*6 + EXTRACT(MINUTE FROM trips.start_time)/10 AS interval, COUNT(*) AS count\
				 FROM trips \
				 WHERE date_part('DOW', trips.start_time) BETWEEN 1 AND 4\
				 AND EXTRACT('EPOCH' FROM (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (EXTRACT('EPOCH' FROM end_time) - 3600*distance/50) * INTERVAL '1 second') - start_time)/60 < 60 \
				 GROUP BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time) \
				 ORDER BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time)")

	timedist = [None] * 24*6
	for interval, count in cur.fetchall():
		timedist[int(interval)] = count
	return timedist
	conn.close()

def fetch_trips(args):
	"""Calculates the OD flows from all o cells to all d cells for a given time-slice
	hour using timedist for start time weighting.
	
	Args:
		args: a 3-tuple o, d, where o is a list of the origin cell ids and
		d a list of destination cell ids
		10min interval of the day
	Returns:
		A list of trips 
	"""

	o, d = args #arguments are passed as tuple due to pool.map() limitations
	#print(str(o) + "->" + str(d) + " started")
	start = time.time()

	conn = util.db_connect()
	cur = conn.cursor()

	#fetch all trips
	trips_sql = "SELECT start_antenna, \
						end_antenna, \
						trip_scale_factor,\
				 		EXTRACT(EPOCH FROM start_time) AS start, \
				 		EXTRACT(EPOCH FROM end_time) AS end, \
				 		EXTRACT(EPOCH FROM end_time) - 3600*distance/50 AS start_interval_end \
				 FROM trips_with_factors \
				 WHERE start_antenna IN %s AND end_antenna IN %s \
				 AND EXTRACT(DOW FROM start_time) BETWEEN 1 AND 4"

	cur.execute(trips_sql, (tuple(o),tuple(d)))

	result = []
	for origin, destination, trip_scale_factor, start_time, end_time, start_interval_end in cur.fetchall():
		result.append(((origin,destination), (trip_scale_factor, start_time, end_time, start_interval_end)))

	conn.close()
	return result

def calculate_od(args):
	"""Calculates the OD flows for one OD-pair and inserts into od table.

	Args:
		args: tuple od, values where od is a tuple of origin and destination cell ids (o,d)
			and values a tuple (trip_scale_factor, start_time, end_time, start_interval_end)
	"""

	global timedist

	od, values = args
	o, d = od
	trip_scale_factors, start_times, end_times, start_interval_ends = [list(x) for x in zip(*values)]

	#calculate time distribution
	no_trips = [0] * 24*6 #10min intervals
	for i in range(0, len(start_times)):
		start_interval_length = start_interval_ends[i] - start_times[i] #uncertainty in start time in minutes
		if start_interval_length < 0:
			#This trip must have been made with a speed higher than
            #50km/h since the computed end time is before the start
            #time. Skipping...
			continue
		elif start_interval_length < 60: #only count trips for time dist with precise trip start info
			interval = int(((start_interval_ends[i]/60 - start_interval_length/2) % (24*60))/10) #10min intervals
			no_trips[interval] += 1

	#calculate OD flows
	flows = numpy.array([0.0] * 24)
	for i in range(0, len(start_times)):
		start_interval = int(((start_times[i]/60) % (24*60))/10) #10min intervals
		end_interval = int(((start_interval_ends[i]/60) % (24*60))/10) #10min intervals
		scale_factor = trip_scale_factors[i]
		if scale_factor == 0:
			scale_factor = 1 #no scale factor for this user, count as 1 trip

		weight = [0.0] * 24*6 #weights for the trip in 10min intervals
		weight_function = lambda trips, dist: scale_factor*float(trips)/float(sum(dist[start_interval:end_interval+1])) #scale * trips/total_trips
		if sum(no_trips[start_interval:end_interval+1]) >= 8: #enough time dist for this OD info available
			weight[start_interval:end_interval+1] = [weight_function(trips, no_trips) for trips in no_trips[start_interval:end_interval+1]]
		else: #otherwise use timedist for all trips
			weight[start_interval:end_interval+1] = [weight_function(trips, timedist) for trips in timedist[start_interval:end_interval+1]]
		weight_hours = numpy.array([sum(weight[6*hour:6*hour+6]) for hour in range(0,24)])
		flows += weight_hours #add this (scaled) trip to the od flow

	#Upload OD flows to DB
	data = []
	for interval in range(0,24):
		if flows[interval] > 0:
			data.append((o,d,interval,flows[interval]))

	conn = util.db_connect()
	cur = conn.cursor()
	rows = [cur.mogrify("(%s, %s, %s, %s)", values) for values in data]

	if len(rows) > 0:
		sql = "INSERT INTO od (start_antenna, end_antenna, time_interval, trip_weight) \
			   VALUES " + ", ".join(rows) + ";"
		cur.execute(sql)
		conn.commit()
	cur.close()

	end = time.time()

def signal_handler(signal, frame):
	global mapper, request_stop
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")

mapper = None
timedist = []

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	util.db_login()

	print("Fetching trip time-distribution...")
	timedist = fetch_timedist()

	print("Deleting from OD table...")
	conn = util.db_connect()
	cur = conn.cursor()
	cur.execute("DELETE FROM od")
	conn.commit()

	#calculate OD flows
	print("Calculating OD flows...")
	args = ((o, d) for o, d in util.od_chunks(2))
	mapper = util.MapReduce(fetch_trips, calculate_od, num_workers = 25)
	mapper(args, pipe = True, length = len(config.CELLS)*len(config.CELLS)/200 + len(config.CELLS))

	#calculate_od((1,5,timedist)) #test
