-- SHORTEST path routing algorithm
-- assigns flows to the road network from all %(orig_cells)s to all %(dest_cells)s during interval %(interval)s
-- returns a result set containing a flow and an array of link ids (hh_2po_4pgr.id) that the flow should be allocated to
-- 
-- SHORTEST path routing just assigns the complete flow to the shortest path between origin and destination cell
-- disregarding the cellpath data

SELECT (route_with_waypoints(array_append(array_prepend(best_startpoint(ARRAY[orig_cell, dest_cell]), 
														ARRAY[]::int[]), 
														best_endpoint(ARRAY[orig_cell, dest_cell])))).edges AS links,
	   flow AS flow 
FROM od
WHERE 	od.orig_cell = ANY(%(orig_cells)s)
	AND od.dest_cell = ANY(%(dest_cells)s)
	AND od.interval = %(interval)s