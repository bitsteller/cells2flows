# coding=utf-8
import unittest

import config, util

from witica.source import SourceItemList

class TestVerifyDB(unittest.TestCase):
	"""Runs several test to verify the integrity of the database
	"""
	def setUp(self):
		self.conn = util.db_connect()
		self.cur = self.conn.cursor()

	def tearDown(self):
		pass

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