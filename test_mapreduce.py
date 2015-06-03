import util, random, config

def mapf(x):
	return [(x,1)]

def redf(x):
	k,v = x
	return (k,sum(v))

#test mapreduce
mapper = util.MapReduce(mapf,redf, num_workers = 4)

a = [random.randint(0,100) for x in xrange(0,100000)]
print(mapper(a, chunksize = 100))

#Test map
mapper = util.ParMap(mapf, num_workers = 4)
a = [random.randint(0,100) for x in xrange(0,100000)]
print(mapper(a, chunksize = 100))

#test od chunks
def od_chunks():
	for origin in [1,2,3,4,5]:
		for destinations in util.chunks([1,2,3,4,5], 2):
			yield ([origin], destinations)
args = ((o, d) for o, d in od_chunks())
print([x for x in args])