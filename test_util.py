# coding=utf-8
import unittest, random

import config, util

from witica.source import SourceItemList

def mapf(x):
	return [(x,1)]

def redf(x):
	k,v = x
	return (k,sum(v))

class TestUtil(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_map(self):
		l = [1]*90000 + [2]*5000
		random.shuffle(l)

		mapper = util.ParMap(mapf, num_workers = 4)
		r = mapper(l, chunksize = 100)
		self.assertEqual(90000, sum([1 for kv in r if kv == [(1,1)]]))
		self.assertEqual(5000, sum([1 for kv in r if kv == [(2,1)]]))

	def test_mapreduce(self):
		l = [1]*90000 + [2]*5000
		random.shuffle(l)

		mapper = util.MapReduce(mapf,redf, num_workers = 4)
		r = mapper(l, chunksize = 100)
		
		self.assertEqual(set([(1,90000), (2,5000)]), set(r))

	def test_odchunks(self):
		pass

