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

	def test_all_cellpaths_have_segments(self):
		sql = "	SELECT COUNT(*) \
				FROM od, LATERAL getTopCellpaths(od.orig_cell, od.dest_cell, 1) cp \
				WHERE NOT EXISTS(SELECT * FROM cellpath_segment WHERE cellpath_segment.cellpath = cp.cellpath)"
		self.cur.execute(sql)
		cellpaths_with_missing_segments = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_segments)

	def test_all_waypoints_found(self):
		sql = "	SELECT COUNT(*) \
				FROM od, LATERAL getTopCellpaths(od.orig_cell, od.dest_cell, 1) cp \
				WHERE EXISTS(SELECT 1 FROM unnest(get_waypoints(cp.cellpath)) w(p) WHERE p IS NULL) \
					OR array_length(get_waypoints(cp.cellpath),1) != array_length(cp.cellpath,1) - 2 \
					OR get_waypoints(cp.cellpath) IS NULL"
		self.cur.execute(sql)
		cellpaths_with_missing_waypoints = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_waypoints)