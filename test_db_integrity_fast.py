# coding=utf-8
import unittest, os.path

import config, util

print("INFO: Not all data is verified, run slow tests additionally to run complete verification.")
print("Running fast tests (can take a few minutes)...")

class TestVerifyDBFast(unittest.TestCase):
	"""Runs several test to verify the integrity of the database. 
	Caution! Some tests only check a small random selected portion of the data, 
	please run slow tests additionally to check all data.
	"""
	def setUp(self):
		self.conn = util.db_connect()
		self.cur = self.conn.cursor()

	def tearDown(self):
		self.conn.close()

	def test_config_ok_general_settings(self):
		self.assertTrue(hasattr(config, "USER"))
		self.assertTrue(hasattr(config, "USER"))
		self.assertTrue(hasattr(config, "PORT"))
		if hasattr(config, "PYOSRM"):
			self.assertTrue(hasattr(config, "PYOSRM_FILE"))
			self.assertTrue(os.path.exists(config.PYOSRM_FILE))

	def test_config_ok_load_files_found(self):
		self.assertTrue(hasattr(config, "ANTENNA_FILE"))
		self.assertTrue(os.path.exists(config.ANTENNA_FILE))

		self.assertTrue(hasattr(config, "TRIPS_FILE"))
		self.assertTrue(os.path.exists(config.TRIPS_FILE))

		self.assertTrue(hasattr(config, "TAZ_FILE"))
		self.assertTrue(os.path.exists(config.TAZ_FILE))

		self.assertTrue(hasattr(config, "OD_FILE"))
		self.assertTrue(os.path.exists(config.OD_FILE))

		self.assertTrue(hasattr(config, "MATSIM_FILE"))
		self.assertTrue(os.path.exists(config.MATSIM_FILE))


	def test_config_ok_computation_parameters(self):
		self.assertTrue(hasattr(config, "MIN_ANTENNA_DIST"))
		self.assertTrue(config.MIN_ANTENNA_DIST >= 1)
		self.assertTrue(config.MIN_ANTENNA_DIST <= 10000)

		self.assertTrue(hasattr(config, "MAX_CELLPATHS"))
		self.assertTrue(config.MAX_CELLPATHS >= 1)
		self.assertTrue(config.MAX_CELLPATHS <= 100)

		self.assertTrue(hasattr(config, "ROUTE_ALGORITHM"))
		self.assertTrue(config.ROUTE_ALGORITHM.upper() in ["SHORTEST", "LAZY", "STRICT"])
		
		self.assertTrue(hasattr(config, "SIMPLIFICATION_TOLERANCE"))
		self.assertTrue(config.SIMPLIFICATION_TOLERANCE >= 0.01)
		self.assertTrue(config.SIMPLIFICATION_TOLERANCE <= 0.1)		

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
		sql = "	SELECT pgr_createTopology('hh_2po_4pgr', 0.000002, 'geom_way'); \
				SELECT pgr_analyzeGraph('hh_2po_4pgr', 0.000002, 'geom_way');"
		self.cur.execute(sql)
		result = self.cur.fetchone()[0]
		if result == "FAIL":
			print("Road network (hh_2po_4pgr) is invalid.")
			print(self.conn.notices[-1])
		self.assertEqual("OK", result)

	def test_road_network_simplified_ok(self):
		sql = "	SELECT pgr_createTopology('hh_2po_4pgr_lite', 0.000002, 'geom_way'); \
				SELECT pgr_analyzeGraph('hh_2po_4pgr_lite', 0.000002, 'geom_way');"
		self.cur.execute(sql)
		result = self.cur.fetchone()[0]
		if result == "FAIL":
			print("Road network (hh_2po_4pgr_lite) is invalid.")
			print(self.conn.notices[-1])
		self.assertEqual("OK", result)

	def test_od_convert_flow_conservation(self):
		"""Checks if no major flows where lost during OD matrix conversion. 
		Flows can be lost if certain TAZs are not touched by any Voronoi cell."""

		sql = "SELECT (SELECT SUM(flow) FROM taz_od) AS total_taz_od_flow, (SELECT SUM(flow) FROM od) AS total_od_flow"
		self.cur.execute(sql)
		total_taz_od_flow, total_od_flow = self.cur.fetchone()

		if total_taz_od_flow != total_od_flow:
			print("WARNING: Total OD flow in cell od matrix diverges from orignal TAZ based OD matrix by " + str(100*(total_od_flow-total_taz_od_flow)/total_taz_od_flow) + "%")
		self.assertTrue(total_od_flow < 1.05*total_taz_od_flow) #allow up to 5% difference
		self.assertTrue(total_od_flow > 0.95*total_taz_od_flow) #allow up to 5% difference

	def test_all_od_pairs_have_cellpath(self):
		sql = "SELECT COUNT(*) FROM (SELECT * FROM od ORDER BY random() LIMIT 1000) AS od WHERE NOT EXISTS (SELECT * FROM getTopCellpaths(od.orig_cell, od.dest_cell, 1))"
		self.cur.execute(sql)
		od_pairs_with_missing_cellpath = self.cur.fetchone()[0]
		self.assertEqual(0, od_pairs_with_missing_cellpath)

	def test_all_cellpaths_have_segments(self):
		sql = "	SELECT COUNT(*) \
				FROM (SELECT * FROM od ORDER BY random() LIMIT 1000) AS od, LATERAL getTopCellpaths(od.orig_cell, od.dest_cell, 1) cp \
				WHERE NOT EXISTS(SELECT * FROM cellpath_segment WHERE cellpath_segment.cellpath = cp.cellpath)"
		self.cur.execute(sql)
		cellpaths_without_segments = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_without_segments)

	def test_number_of_segments_correct(self):
		sql = "	SELECT COUNT(*) \
				FROM (SELECT * FROM simple_cellpath ORDER BY random() LIMIT 1000) AS scp \
				WHERE array_length(scp.simple_cellpath,1) <> (SELECT COUNT(*) FROM cellpath_segment WHERE cellpath_segment.cellpath = scp.cellpath) + 1"
		self.cur.execute(sql)
		cellpaths_with_missing_or_extra_segments = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_or_extra_segments)	

	def test_all_waypoints_found(self):
		sql = "	SELECT COUNT(*) \
				FROM (SELECT * FROM od ORDER BY random() LIMIT 1000) AS od, LATERAL getTopCellpaths(od.orig_cell, od.dest_cell, 1) cp \
				WHERE EXISTS(SELECT 1 FROM unnest(get_waypoints(cp.cellpath)) w(p) WHERE p IS NULL) \
					OR array_length(get_waypoints(cp.cellpath),1) != array_length(cp.cellpath,1) - 2 \
					OR get_waypoints(cp.cellpath) IS NULL"
		self.cur.execute(sql)
		cellpaths_with_missing_waypoints = self.cur.fetchone()[0]
		self.assertEqual(0, cellpaths_with_missing_waypoints)

	def test_all_startpoints_calculated(self):
		sql = "SELECT COUNT(DISTINCT cellpath[1:2]) \
				FROM cellpath_dist \
				WHERE 	array_length(cellpath, 1) >= 2 AND\
					NOT EXISTS(SELECT * FROM best_startpoint WHERE best_startpoint.part = cellpath[1:2])"
		self.cur.execute(sql)
		self.assertEqual(0, self.cur.fetchone()[0])

	def test_all_endpoints_calculated(self):
		sql = "SELECT COUNT(DISTINCT cellpath[array_upper(cellpath,1)-1:array_upper(cellpath,1)]) \
		FROM cellpath_dist \
		WHERE 	array_length(cellpath, 1) >= 2 AND \
				NOT EXISTS(SELECT * FROM best_endpoint WHERE best_endpoint.part = cellpath[array_upper(cellpath,1)-1:array_upper(cellpath,1)])"
		self.cur.execute(sql)
		self.assertEqual(0, self.cur.fetchone()[0])

	def test_all_waypoints_calculated(self):
		sql = "SELECT COUNT(*) FROM cellpath_parts WHERE NOT EXISTS(SELECT * FROM waypoints WHERE waypoints.part = cellpath_parts.part)"
		self.cur.execute(sql)
		self.assertEqual(0, self.cur.fetchone()[0])

	def test_all_odpairs_covered(self):
		self.cur.execute("SELECT SUM(flow) FROM od")
		total_od_flow = self.cur.fetchone()[0]

		covered_od_flow = 0
		for o_cells, d_cells in util.od_chunks(chunksize = 5000):
			self.cur.execute("SELECT SUM(flow) FROM od WHERE od.orig_cell = ANY(%(o_cells)s) AND od.dest_cell = ANY(%(d_cells)s)",
			{
				"o_cells": o_cells,
				"d_cells": d_cells
			})
			covered_od_flow += self.cur.fetchone()[0]

		self.assertAlmostEqual(total_od_flow, covered_od_flow)

	def test_avg_number_segments_ok(self):
		self.cur.execute("SELECT 1+AVG(segment_id) FROM cellpath_segment")
		avg_num_segments = self.cur.fetchone()[0]
		
		#warning levels
		if avg_num_segments < 1.5:
			print("WARNING: Average number of segments very low (" + str(avg_num_segments) + "). You should probably lower the SIMPLIFICATION_TOLERANCE parameter!")
		elif avg_num_segments > 6:
			print("WARNING: Average number of segments very high (" + str(avg_num_segments) + "). You should probably increase the SIMPLIFICATION_TOLERANCE parameter!")

		#critical levels
		self.assertTrue(avg_num_segments > 1.2)
		self.assertTrue(avg_num_segments < 12.0)

	def test_route_lazy_all_flow_assigned(self):
		self.validate_route_algo_all_flow_assigned("LAZY")

	def test_route_strict_all_flow_assigned(self):
		self.validate_route_algo_all_flow_assigned("STRICT")

	#def test_route_shortest_all_flow_assigned(self):
	#	self.validate_route_algo_all_flow_assigned("SHORTEST")

	def validate_route_algo_all_flow_assigned(self, algorithm):
		for od_data in util.get_random_od_data(50):
			assigned_flow = 0
			for links, flow in self.assign_flow(od_data, algorithm):
				self.assertNotEqual(None, links)
				self.assertTrue(len(links) > 0)
				assigned_flow += flow
			self.assertAlmostEqual(od_data["flow"], assigned_flow)

	def assign_flow(self,od_data, algorithm):
		od_data["max_cellpaths"] = config.MAX_CELLPATHS
		flows_sql = open("SQL/04_Routing_Network_Loading/algorithms/" + algorithm.upper() + "/flows.sql", 'r').read()
		self.cur.execute(flows_sql, od_data)
		result = self.cur.fetchall()
		self.conn.commit()
		return result
