# coding=utf-8
import unittest

import config, util

class TestVerifyDBSlow(unittest.TestCase):
	"""Runs additional test on the complete dataset. These tests can take very long time to execute.
	"""
	def setUp(self):
		self.conn = util.db_connect()
		self.cur = self.conn.cursor()

	def tearDown(self):
		self.conn.close()

	def test_all_od_pairs_have_cellpath(self):
		sql = "SELECT COUNT(*) FROM od WHERE NOT EXISTS (SELECT * FROM getTopCellpaths(od.orig_cell, od.dest_cell, 1))"
		self.cur.execute(sql)
		od_pairs_with_missing_cellpath = self.cur.fetchone()[0]
		self.assertEqual(0, od_pairs_with_missing_cellpath)

