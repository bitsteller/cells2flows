import collections, sys, itertools, multiprocessing, re, datetime, time
import psycopg2

import config

def parse_trajectory(linestr):
	"""Reads a line from csv file and parses the trajectory
	Args:
		linestr: one-line string from the a STEM csv file
	Returns:
		A tuple (userid, sequence) where sequence is a list of tuples (lat, lon, timestr) for each position in the trajectory"""

	userid, trajectory = re.match(r"([0-9]+)[\s]+\[([\S\s]+)\]",linestr).groups()
	sequence = [] #list of tuples containing userid, lat, lon, time
	for lat, lon, timestr in re.findall(r"\[\[([\-0-9.]+), ([\-0-9.]+)\], '([0-9:\- ]+)'\]",trajectory):
		t = datetime.datetime.strptime(timestr.rpartition("-")[0], "%Y-%m-%d %H:%M:%S")
		sequence.append((float(lat), float(lon), t))
	return (userid, sequence)

def parse_antenna(linestr):
	"""Reads a line from antenna csv file
	Args:
		linestr: one-line string from the a antenna csv file
	Returns:
		A list of tuples (lon, lat, srid) containing the antenna positions extracted, 
		where lat/lon are in the coordinate system given by the SRID; 
		alternatively a tuple (id, lon, lat, srid) if a specific id shoul be assigned to the antenna"""

	lon, lat = re.match(r"([0-9.]+),([0-9.]+)",linestr).groups()
	return (float(lon), float(lat), 32611)

def parse_trip(linestr):
	"""Reads a line from a trip csv file
	Args:
		linestr: one-line string from the a trip csv file
	Returns:
		A list of tuples (userid, cellpath) containing the user id and a list of visited cell on the trip"""

	try:
		data = re.match(r"([0-9]+),([01]),([0-9.]+),([0-9.]+),([0-9 ]*)",linestr).groups()
		userid, commute_direction, orig_TAZ, dest_TAZ, cellpathstr = data
		try:
			cellpath = [int(cell) for cell in cellpathstr.strip(" ").split(" ")]
			return (userid, cellpath)
		except Exception, e:
			print("Line '" + linestr + "' could will be ignored, because '" + cellpathstr + "' is not a valid cellpath")
			return None
	except Exception, e:
		print("Line '" + linestr + "' has an invalid syntax and will be ignored.")
		return None

def confirm(prompt_str, allow_empty=False, default=False):
	"""Prompts the user to confirm an action and returns the users decision.
	Args:
		prompt_str:
			A description of the action that the user should confirm (for example "Delete file x?")
		allow_empty:
			If true, the default action assumed, even if the user just pressed enter
		default:
			The default action (true: accept, false: decline)
	Returns:
		True if the user accepted the action and false if not.
	"""

	fmt = (prompt_str, 'y', 'n') if default else (prompt_str, 'n', 'y')
	if allow_empty:
		prompt = '%s [%s]|%s: ' % fmt
	else:
		prompt = '%s %s|%s: ' % fmt
	while True:
		ans = raw_input(prompt).lower()
		if ans == '' and allow_empty:
			return default
		elif ans == 'y':
			return True
		elif ans == 'n':
			return False
		else:
			print("Please enter y or n.")

def chunks(seq, n):
	"""Partionions a sequence into chunks
	Args:
		seq: the sequence to split in chunks
		n: the maximum chunksize
	Return:
		A generator that yields lists containing chunks of the original sequence
	"""

	return (seq[i:i+n] for i in xrange(0, len(seq), n))

def od_chunks(chunksize = 200):
	"""Returns a generator that returns OD pair chunks based on the cell ids

	Returns:
		A generator that returns tuples of the form ([list of origins], [list of destinations])"""

	for origin in config.CELLS:
		for destinations in chunks(config.CELLS, chunksize):
			yield ([origin], destinations)

def db_login(force_password=False):
	"""Makes sure that config.PASSWORD is set to the database password. 
	If config.PASSWORD is alread defined, this function will not do anything. Otherwise
	it will try to fetch the password from the systems keychain. If no password is stored
	in the keychain yet, the user is prompted to enter the password and optinally store it
	in the system keychain.

	Args:
		force_password: If set to True, the user is prompted even if the password 
		is stored in the keychain (useful if the password needs to be changed
	"""

	if "PASSWORD" in dir(config) != None: #password already set in config.py
		return
	
	import keyring, getpass
	config.PASSWORD = keyring.get_password(config.DATABASE, config.USER)
	if config.PASSWORD == None or force_password == True:
		while 1:
			print("A password is needed to continue. Please enter the password for")
			print(" * service: postgresql")
			print(" * database: " + config.DATABASE)
			print(" * user: " + config.USER)
			print("to continue.")
			config.PASSWORD = getpass.getpass("Please enter the password:\n")
			if config.PASSWORD != "":
				break
			else:
				print ("Authorization failed (no password entered).")
		# store the password
		if confirm("Do you want to securely store the password in the keyring of your operating system?",default=True):
			keyring.set_password(config.DATABASE, config.USER, config.PASSWORD)
			print("Password has been stored. You will not have to enter it again the next time. If you need to edit the password use the keychain manager of your system.")

def db_connect():
	return psycopg2.connect("dbname=" + config.DATABASE + " user=" + config.USER + " password=" + config.PASSWORD + " host=localhost " + " port=" + str(config.PORT))

def partition(mapped_values):
	"""Organize the mapped values by their key.
	Returns an unsorted sequence of tuples with a key and a sequence of values.

	Args: 
		mapped_values: a list of tuples containing key, value pairs

	Returns:
		A list of tuples (key, [list of values])
	"""

	partitioned_data = collections.defaultdict(list)
	for key, value in mapped_values:
		partitioned_data[key].append(value)
	return partitioned_data.items()

class MapReduce(object):
	def __init__(self, map_func, reduce_func, num_workers=multiprocessing.cpu_count()):
		"""
		map_func

		  Function to map inputs to intermediate data. Takes as
		  argument one input value and returns a tuple with the key
		  and a value to be reduced.
		
		reduce_func

		  Function to reduce partitioned version of intermediate data
		  to final output. Takes as argument a key as produced by
		  map_func and a sequence of the values associated with that
		  key.
		 
		num_workers

		  The number of workers to create in the pool. Defaults to the
		  number of CPUs available on the current host.
		"""
		self.map_func = map_func
		self.reduce_func = reduce_func
		self.mappool = multiprocessing.Pool(num_workers)
		self.reducepool = multiprocessing.Pool(num_workers)
		self.request_stop = False
		self.num_workers = num_workers

	def stop(self):
		self.request_stop = True
		self.mappool.close()
		self.reducepool.close()
	
	def __call__(self, inputs, chunksize=10, pipe=False, length = None):
		"""Process the inputs through the map and reduce functions given.
		
		inputs
		  An iterable containing the input data to be processed.
		
		chunksize=1
		  The portion of the input data to hand to each worker.  This
		  can be used to tune performance during the mapping phase.

		pipe = False
		  When set to true, key/value pairs are passed from map directly to reduce function just once. 
		  Only applicable, when all values for every key are generated at once (no partioning or 
		  reducing of the result of reduce)
		"""

		if length == None:
			length = len(inputs)

		#map
		start = time.time()
		tasks_finished = 0
		result = []
		mapped = []
		for response in self.mappool.imap_unordered(self.map_func, inputs, chunksize=chunksize):
			if pipe:
				mapped.extend(response)
			else:
				result.extend(response)
			if self.request_stop:
				return

			if tasks_finished % (chunksize*self.num_workers) == 0:
				#partition
				partitioned_data = []
				if pipe:
					partitioned_data = partition(mapped)
				else:
					partitioned_data = partition(result)
				#reduce
				reduced = self.reducepool.map(self.reduce_func, partitioned_data)
				if pipe:
					result.extend(reduced)
					mapped = []
				else:
					result = reduced

			tasks_finished += 1
			est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-start)/tasks_finished*(length-tasks_finished))
			sys.stderr.write('\rdone {0:%}'.format(float(tasks_finished)/length) + "  ETA " + est.strftime("%Y-%m-%d %H:%M"))

		#partition
		partitioned_data = []
		if pipe:
			partitioned_data = partition(mapped)
		else:
			partitioned_data = partition(result)

		#reduce
		reduced = map(self.reduce_func, partitioned_data)
		if pipe:
			result.extend(reduced)
		else:
			result = reduced
			mapped = []

		print("")
		return result

def void(arg):
		return arg

class ParMap(MapReduce):
	def __init__(self, map_func, num_workers=multiprocessing.cpu_count()):
		"""
		map_func

		  Function to map inputs to intermediate data. Takes as
		  argument one input value and returns a tuple with the key
		  and a value to be reduced.
		 
		num_workers

		  The number of workers to create in the pool. Defaults to the
		  number of CPUs available on the current host.
		"""
		self.map_func = map_func
		self.mappool = multiprocessing.Pool(num_workers)
		self.request_stop = False
		self.num_workers = num_workers

	def stop(self):
		self.request_stop = True
		self.mappool.close()

	def __call__(self, inputs, chunksize=10, length = None):
		"""Process the inputs through the map and reduce functions given.
		
		inputs
		  An iterable containing the input data to be processed.
		
		chunksize=1
		  The portion of the input data to hand to each worker.  This
		  can be used to tune performance during the mapping phase.
		"""

		if length == None:
			length = len(inputs)

		#map
		tasks_finished = 0
		start = time.time()
		result = []
		for response in self.mappool.imap_unordered(self.map_func, inputs, chunksize=chunksize):
			result.append(response)
			if self.request_stop:
				return

			tasks_finished += 1
			est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-start)/tasks_finished*(length-tasks_finished))
			sys.stderr.write('\rdone {0:%}'.format(float(tasks_finished)/length) + "  ETA " + est.strftime("%Y-%m-%d %H:%M"))

		print("")
		return result