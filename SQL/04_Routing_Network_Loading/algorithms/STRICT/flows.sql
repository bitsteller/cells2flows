-- STRICT Voronoi routing algorithm
-- assigns flows to the road network from all %(orig_cells)s to all %(dest_cells)s during interval %(interval)s
-- using the top %(max_cellpaths)s cellpaths per OD pair
-- returns a result set containing a flow and an array of link ids (hh_2po_4pgr.id) that the flow should be allocated to
-- 
-- STRICT Voronoi routing uses a waypoint in each cell in the cellpath to force the traffic to enter the Voronoi cell

WITH cellpath_flow AS (	SELECT od.orig_cell, od.dest_cell, cellpath, cellpath.share * flow AS flow
						FROM od, getTopCellpaths(od.orig_cell, od.dest_cell, %(max_cellpaths)s) cellpath
						WHERE od.orig_cell = ANY(%(orig_cells)s)
							AND od.dest_cell = ANY(%(dest_cells)s)
							AND od.interval = %(interval)s
					   )
SELECT (routeStrict(cellpath_flow.cellpath) AS links,
	   flow AS flow FROM cellpath_flow