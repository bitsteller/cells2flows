# coding=utf-8
import unittest

import config, util

class TestVerifyDBFast(unittest.TestCase):
	"""Runs several test to verify the integrity of the database. 
	Some tests only check a portion of the data, please run slow tests additionally to check all data.
	"""
	def setUp(self):
		self.conn = util.db_connect()
		self.cur = self.conn.cursor()

	def tearDown(self):
		self.conn.close()

	def test_all_vertices_reachable(self):
		sql = """WITH reachable_vertices AS (
					WITH targets AS (
					         SELECT array_agg(hh_2po_4pgr_vertices.id) AS ids
					           FROM hh_2po_4pgr_vertices
					        )
					 SELECT DISTINCT cr.id1 AS id,
					    hh_2po_4pgr_vertices.geom
					   FROM targets,
					    LATERAL pgr_kdijkstrapath('SELECT id, source, target, cost, reverse_cost FROM hh_2po_4pgr'::text, (SELECT id FROM hh_2po_4pgr_vertices LIMIT 1), targets.ids, true, true) cr(seq, id1, id2, id3, cost)
					     JOIN hh_2po_4pgr_vertices ON hh_2po_4pgr_vertices.id = cr.id1
					   WHERE cost >= 0
				)
				SELECT hh_2po_4pgr_vertices.id 
				FROM hh_2po_4pgr_vertices
				WHERE NOT EXISTS(SELECT * FROM reachable_vertices WHERE reachable_vertices.id = hh_2po_4pgr_vertices.id)"""
		self.cur.execute(sql)
		for (vid,) in self.cur:
			print("Vertex " + str(vid) + " not reachable")
		self.assertEqual(self.cur.rowcount, 0)

	def test_road_networks_ok(self):
		#check full network
		sql = "	SELECT pgr_createTopology('hh_2po_4pgr', 0.000002, 'geom_way'); \
				SELECT pgr_analyzeGraph('hh_2po_4pgr', 0.000002, 'geom_way');"
		self.cur.execute(sql)
		result = self.cur.fetchone()[0]
		if result == "FAIL":
			print("Road network (hh_2po_4pgr) is invalid.")
			print(self.conn.notices[-1])
		self.assertEqual("OK", result)

	def test_road_network_simplified_ok(self):
		#check simplified network
		sql = "	SELECT pgr_createTopology('hh_2po_4pgr_lite', 0.000002, 'geom_way'); \
				SELECT pgr_analyzeGraph('hh_2po_4pgr_lite', 0.000002, 'geom_way');"
		self.cur.execute(sql)
		result = self.cur.fetchone()[0]
		if result == "FAIL":
			print("Road network (hh_2po_4pgr_lite) is invalid.")
			print(self.conn.notices[-1])
		self.assertEqual("OK", result)

	def test_od_convert_flow_conservation(self):
		"""Checks if no major flows where lost during OD matrix conversion"""

		sql = "SELECT (SELECT SUM(flow) FROM taz_od) AS total_taz_od_flow, (SELECT SUM(flow) FROM od) AS total_od_flow"
		self.cur.execute(sql)
		total_taz_od_flow, total_od_flow = self.cur.fetchone()

		if total_taz_od_flow != total_od_flow:
			print("WARNING: Total OD flow in cell od matrix diverges from orignal TAZ based OD matrix by " + str(100*(total_od_flow-total_taz_od_flow)/total_taz_od_flow) + "%.")
		self.assertTrue(total_od_flow < 1.05*total_taz_od_flow) #allow up to 5% difference
		self.assertTrue(total_od_flow > 0.95*total_taz_od_flow) #allow up to 5% difference

	def test_route_lazy_all_flow_assigned(self):
		pass

	def test_route_strict_all_flow_assigned(self):
		pass

	def test_route_shortest_all_flow_assigned(self):
		pass

	def test_all_cellpaths_have_segments(self):
		pass

	def test_all_od_pairs_have_cellpath(self):
		sql = "SELECT COUNT(*) FROM (SELECT * FROM od ORDER BY random() LIMIT 1000) AS od WHERE NOT EXISTS (SELECT * FROM getTopCellpaths(od.orig_cell, od.dest_cell, 1))"
		self.cur.execute(sql)
		od_pairs_with_missing_cellpath = self.cur.fetchone()[0]
		self.assertEqual(0, od_pairs_with_missing_cellpath)

