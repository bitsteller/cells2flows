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
		cellpaths_without_segments = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_without_segments)

	def test_number_of_segments_correct(self):
		sql = "	SELECT COUNT(*) \
				FROM simple_cellpath AS scp \
				WHERE array_length(scp.simple_cellpath,1) <> (SELECT COUNT(*) FROM cellpath_segment WHERE cellpath_segment.cellpath = scp.cellpath)" - 1
		self.cur.execute(sql)
		cellpaths_with_missing_or_extra_segments = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_or_extra_segments)	

	def test_all_waypoints_found(self):
		sql = "	SELECT COUNT(*) \
				FROM od, LATERAL getTopCellpaths(od.orig_cell, od.dest_cell, 1) cp \
				WHERE EXISTS(SELECT 1 FROM unnest(get_waypoints(cp.cellpath)) w(p) WHERE p IS NULL) \
					OR array_length(get_waypoints(cp.cellpath),1) != array_length(cp.cellpath,1) - 2 \
					OR get_waypoints(cp.cellpath) IS NULL"
		self.cur.execute(sql)
		cellpaths_with_missing_waypoints = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_waypoints)

	def test_route_lazy_all_flow_assigned(self):
		self.validate_route_algo_all_flow_assigned("LAZY")

	def test_route_strict_all_flow_assigned(self):
		self.validate_route_algo_all_flow_assigned("STRICT")

	#def test_route_shortest_all_flow_assigned(self):
		#self.validate_route_algo_all_flow_assigned("SHORTEST")

	def validate_route_algo_all_flow_assigned(self, algorithm):
		for od_data in util.get_random_od_data(10000):
			assigned_flow = 0
			for links, flow in self.assign_flow(od_data, algorithm):
				self.assertNotEqual(None, links)
				self.assertTrue(len(links) > 0)
				assigned_flow += flow
			self.assertEqual(od_data["flow"], assigned_flow)

	def assign_flow(self,od_data, algorithm):
		od_data["max_cellpaths"] = config.MAX_CELLPATHS
		flows_sql = open("SQL/04_Routing_Network_Loading/algorithms/" + algorithm.upper() + "/flows.sql", 'r').read()
		self.cur.execute(flows_sql, od_data)
		result = self.cur.fetchall()
		self.conn.commit()
		return result