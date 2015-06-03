import datetime, time, signal, re, sys, os, shutil
import psycopg2 #for postgres DB access
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as mdates
from geopy.distance import vincenty
from mpl_toolkits.axes_grid.axislines import Subplot


import util, config #local modules

#time resolution analysis
def user_trajectories(args):
	line, hours = args #pass args as tuple due to paralell pool limitations
	userid, sequence = util.parse_trajectory(line)

	result = []
	for lat, lon, t in sequence:
		if t.hour in hours:
			result.append((userid, (unix_time(t),unix_time(t),1)))

	return result

def min_max_count(item):
	userid, values = item
	min_t = min([v[0] for v in values])
	max_t = max([v[1] for v in values])
	n = sum([v[2] for v in values])

	return (userid, (min_t, max_t, n))

def avg(item):
	userid, values = item
	min_t, max_t, n = values
	if max_t > min_t:
		return (userid, (float(max_t)-float(min_t))/float(n))
	else:
		return None

#cell histograms
def user_positions(args):
	line, hours = args #pass args as tuple due to paralell pool limitations
	userid, sequence = util.parse_trajectory(line)

	result = []
	for lat, lon, t in sequence:
		if t.hour in hours:
			result.append(((userid, lat, lon), 1))

	return result

def addByKey(item):
	key, values = item
	return (key, sum(values))

def group_user(item):
	key, count = item
	userid, lat, lon = key
	return (userid, (lat, lon, count))

def keep(item):
	return item

#trajectory timeline
def user_trajectory_positions(args):
	line, hours = args #pass args as tuple due to paralell pool limitations
	userid, sequence = util.parse_trajectory(line)

	result = []
	for lat, lon, t in sequence:
		if t.hour in hours:
			result.append((userid, (unix_time(t), lat, lon)))

	return result

def prevpos(posdata, time):
	i = len(posdata) - 1
	while i >= 0:
		if posdata[i][0] <= time:
			return posdata[i]
		i = i - 1
	return None

def nextpos(posdata, time):
	i = 0
	while i < len(posdata):
		if posdata[i][0] >= time:
			return posdata[i]
		i = i + 1
	return None

def cont_position(posdata):
	#returns a list with a continious position of the user for every minute, None if unkown position
	cont_pos = []
	for t in range(24*60):
		prev = prevpos(posdata, time.mktime(config.SAMPLE_DAY.timetuple())+60*t)
		next = nextpos(posdata, time.mktime(config.SAMPLE_DAY.timetuple())+60*t)

		closest = None
		if prev != None and next != None:
			if abs(prev[0]-60*t) <= abs(next[0]-60*t): #select the closest position
				closest = prev
			else:
				closest = next
		elif prev != None:
			closest = prev
		elif next != None:
			closest = next
		else:
			closest = None

		if closest == None: #no position found
			cont_pos.append((None, None, 0.0)) #lat, lon, confidence
		elif abs(closest[0]-(time.mktime(config.SAMPLE_DAY.timetuple())+60*t)) < 10*60: #known position
			cont_pos.append((closest[1], closest[2], 1.0)) #lat, lon, confidence
		elif prev != None and next != None and (prev[1:2] == next[1:2]) and abs(prev[0]-next[0]) < 3*60*60: #probable position, if previous and next cell are the same
			cont_pos.append((closest[1], closest[2], 0.2)) #lat, lon, confidence
		else: #position too old
			cont_pos.append((None, None, 0.0)) #lat, lon, confidence
	
	assert(len(cont_pos) == 24*60)
	return cont_pos

def plot_timeline(userid, posdata, plot_distance = False):
	pos = cont_position(posdata) #transform into continius position
	colorkeys = [lat+lon for lat, lon, confidence in pos if confidence > 0.0 ]
	confidences = [confidence for lat, lon, confidence in pos if confidence > 0.0 ]

	cmap = plt.get_cmap("Paired")
	if plot_distance:
		colorkeys = [vincenty((pos[i][0],pos[i][1]), (pos[i+1][0],pos[i+1][1])).kilometers if pos[i+1][2] > 0.0 else 0 for i in range(len(pos)-1) if pos[i][2] > 0.0] + [0.0]
		cmap = plt.get_cmap("Reds")

	norm = mpl.colors.Normalize()
	sm = mpl.cm.ScalarMappable(norm=norm, cmap=cmap)

	fig = plt.figure(1)
	fig.set_figheight(3)
	startdates = mins2datenum(range(24*60))
	enddates = mins2datenum(range(1,24*60+1))
	xranges = [(s,e-s) for s,e in zip(startdates, enddates)]
	xranges = [xr for xr, p in zip(xranges,pos) if p[2] > 0.0]
	colors = [(r,g,b,a) for (r,g,b,a), confidence in zip(sm.to_rgba(colorkeys), confidences)]

	assert(len(xranges) == len(colors))

	plt.broken_barh([xr for xr, c in zip(xranges,confidences) if c > 0.0], (0,0.5), facecolors = [col for col, c in zip(colors, confidences) if c > 0.0], linewidth = 0.0)
	plt.broken_barh([xr for xr, c in zip(xranges,confidences) if c > 0.2], (0,1), facecolors = [col for col, c in zip(colors, confidences) if c > 0.2], linewidth = 0.0)
	plt.ylim(0,1)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
	plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
	plt.xlim((mdates.date2num(config.SAMPLE_DAY_NO_TZ), mdates.date2num(config.SAMPLE_DAY_NO_TZ + datetime.timedelta(days=1))))
	plt.gcf().autofmt_xdate()

	if plot_distance:
		plt.savefig("figs/dist/" + userid + ".png")
	else:
		plt.savefig("figs/timelines/" + userid + ".png")
	plt.close()	

def mins2datenum(mins):
	dates = [config.SAMPLE_DAY_NO_TZ + datetime.timedelta(minutes = float(m)) for m in mins]
	return [mdates.date2num(item) for item in dates]

def plot_hist_time(userid, posdata):
	pos = cont_position(posdata) #transform into continius position
	cells = sorted(set([(p[0], p[1]) for p in pos if p[2] > 0.0]))
	cell_time_known = [sum([1 for p in pos if p[2] > 0.2 and p[0] == lat and p[1] == lon]) for lat,lon in cells]
	cell_time = [sum([1 for p in pos if p[2] > 0.0 and p[0] == lat and p[1] == lon]) for lat,lon in cells]

	fig = plt.figure(2)
	fig, ax = plt.subplots()
	plt.ylabel("Time in cell [min]")
	plt.bar(range(len(cells)), cell_time, color = (0.0,0.0,0.8,0.2), linewidth= 0.0)
	plt.bar(range(len(cells)), cell_time_known, color = (0.0,0.0,0.8,1.0),  linewidth= 0.0)
	plt.savefig("figs/times/" + userid + ".png")
	plt.close()

# def highpass(x, rc):
# 	y = [0.0] * len(x)
# 	alpha = rc / (rc + 1)
# 	y[0] = x[0]
# 	for i in range(1,len(x)):
# 		y[i] = alpha * y[i-1] + alpha * (x[i] - x[i-1])
# 	return y

def lowpass(x, rc):
	y = [0.0] * len(x)
	alpha = 1 / (rc + 1)
	y[0] = x[0]
	for i in range(1,len(x)):
		y[i] = alpha * x[i] + (1-alpha) * y[i-1]
	return y

def moving_avg(x, window_size):
	limits = lambda i: (max(0, i-(window_size//2)), min(len(x), i + (window_size//2) + 1))
	y = [sum(x[lower:upper])/float(len(x[lower:upper])) for lower, upper in map(limits, range(len(x)))]
	return y

def plot_efficiency(userid, posdata):
	pos = cont_position(posdata) #transform into continius position
	#calculate efficiency
	dist = [vincenty((pos[i][0],pos[i][1]),(pos[i+1][0],pos[i+1][1])).kilometers if pos[i][2] > 0.0 and pos[i+1][2] > 0.0 else 0.0 for i in range(0,len(pos)-1)] #distance in km if consecutive positions available
	efficiencies = []
	for t in range(24*60):
		window = 30 #minutes window left and right
		t1 = t - window
		t2 = t + window

		#search for a position that is 60min ago
		if t1 in range(24*60) and t2 in range(24*60) and pos[t-window][2] > 0.0 and pos[t+window][2] > 0.0 :
			straightDist = vincenty((pos[t-window][0], pos[t-window][1]), (pos[t+window][0], pos[t+window][1])).kilometers
			travelDist = sum(dist[t-window:t+window])
			if travelDist > 0:
				efficiencies.append(min(straightDist/travelDist, 1.0))
			else:
				efficiencies.append(0)
		else:
			efficiencies.append(0)
	
	#detect trips
	lowpass_eff = lowpass(efficiencies, 12.0) #moving_avg(efficiencies, 30) 
	tripstarts = []
	tripends = []
	
	t = 0
	while t < 24*60:
		if lowpass_eff[t] > 0.5: #trip detected, find start and end
			start = t
			while start > 0:
				if lowpass_eff[start] <= 0.2:
					break
				start = start - 1
			end = t
			while end < 24*60:
				if lowpass_eff[end] <= 0.2:
					break
				end = end + 1

			tripstarts.append(start)
			tripends.append(end)
			t = end + 1
		else:
			t = t + 1

	#plot trips
	startdates = mins2datenum(tripstarts)
	enddates = mins2datenum(tripends)
	yrange = [(s,e-s) for s,e in zip(startdates, enddates)]

	fig = plt.figure(3)
	ax = Subplot(fig,111)
	fig.add_subplot(ax)
	ax.axis["right"].set_visible(False)
	ax.axis["top"].set_visible(False)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
	plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
	plt.xlim((mdates.date2num(config.SAMPLE_DAY_NO_TZ), mdates.date2num(config.SAMPLE_DAY_NO_TZ + datetime.timedelta(days=1))))
	plt.ylim((0,1.1))
	plt.gcf().autofmt_xdate()
	plt.ylabel("Efficiency (straightDist/travelDist)")
	plt.broken_barh(yrange, (0,1), facecolor = (1.0,0.5,0.0,0.4), linewidth = 0.0)

	#plot  efficiency
	times = [mdates.date2num(config.SAMPLE_DAY_NO_TZ + datetime.timedelta(minutes = float(m))) for m in range(24*60)]
	assert(len(efficiencies) == len(times))

	plt.plot(times, efficiencies,  linewidth = 2.0, color = "blue")
	plt.plot(times, lowpass_eff,  linewidth = 2.0, color = "red")

	plt.savefig("figs/eff/" + userid + ".png")
	plt.close()

def unix_time(dt):
	epoch = datetime.datetime.utcfromtimestamp(0)
	delta = dt - epoch
	return delta.total_seconds()

def make_empty_dir(path):
	if os.path.isdir(path):
		shutil.rmtree(path)
	os.makedirs(path)

def signal_handler(signal, frame):
	global count_cells, request_stop
	request_stop = True
	print("Aborting (can take a minute)...")

request_stop = False

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C

	#create folders
	make_empty_dir("figs/counts")
	make_empty_dir("figs/times")
	make_empty_dir("figs/timelines")
	make_empty_dir("figs/dist")
	make_empty_dir("figs/eff")

	#connect to db
	util.db_login()

	print("Reading file and search min/max time + count positions...")
	min_max = util.MapReduce(user_trajectories, min_max_count)
	args = ((line, range(0,24)) for line in open(config.SAMPLE_FILENAME, 'r').readlines()) #hours 1-5
	min_max_t = min_max(args, length = config.SAMPLE_SIZE)

	print("Calculate average time resolutions...")
	avg_time_res = map(avg, min_max_t)
	total_avg_time_res = sum([t for userid, t in avg_time_res])/len(avg_time_res)

	print("avg time between positions: " + str(total_avg_time_res))
	fig = plt.figure()
	plt.hist([v[1] for v in avg_time_res], bins = 50)
	plt.close()

	print("Reading file and counting cell occurencies...")
	cell_occ = util.MapReduce(user_positions, addByKey)
	args = ((line, range(0,24)) for line in open(config.SAMPLE_FILENAME, 'r').readlines()) #hours 1-5
	occ = cell_occ(args, length = config.SAMPLE_SIZE)

	print("Creating cell occurrency graphs...")
	for userid, celldata in util.partition(map(group_user, occ)):
		cells = sorted(set([(p[0], p[1]) for p in celldata]))
		counts = [0] * len(cells)
		for lat, lon, count in celldata:
			counts[cells.index((lat,lon))] += count
		fig = plt.figure()
		plt.ylabel("# occurrences")
		plt.bar([c[0] for c in enumerate(counts)], counts, color = (0.0,0.0,0.8,1.0),  linewidth= 0.0)
		plt.savefig("figs/counts/" + userid + ".png")
		plt.close()

	print("Creating timeline graphs...")
	args = ((line, range(0,24)) for line in open(config.SAMPLE_FILENAME, 'r').readlines()) #hours 1-5
	for i, traj in enumerate(map(user_trajectory_positions, args)):
		userid = traj[0][0]
		posdata = [(p[1][0], p[1][1], p[1][2]) for p in traj]
		posdata = sorted(posdata, cmp = lambda x, y: cmp(x[0],y[0]))
		times = [p[0] for p in posdata]

		#timeline
		plot_timeline(userid, posdata)

		#distance timeline
		plot_timeline(userid, posdata, plot_distance=True)

		#histogram by time
		plot_hist_time(userid, posdata)

		#efficiency
		plot_efficiency(userid, posdata)
		
		sys.stderr.write('\rdone {0:%}'.format(float(i+1)/(config.SAMPLE_SIZE)))

	print("")

	plt.show()

