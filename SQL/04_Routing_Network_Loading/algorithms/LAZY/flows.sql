-- LAZY Voronoi routing algorithm
-- assigns flows to the road network from all %(orig_cells)s to all %(dest_cells)s during interval %(interval)s
-- using the top %(max_cellpaths)s cellpaths per OD pair
-- returns a result set containing a flow and an array of link ids (hh_2po_4pgr.id) that the flow should be allocated to
-- 
-- LAZY Voronoi routing uses waypoints only in few selected cells. 
-- Between the these waypoints the route is computed using adjusted weights based on the visited cells in the cellpath

WITH cellpath_flow AS (	SELECT od.orig_cell, od.dest_cell, cellpath, cellpath.share * flow AS flow
						FROM od, getTopCellpaths(od.orig_cell, od.dest_cell, %(max_cellpaths)s) cellpath
						WHERE od.orig_cell = ANY(%(orig_cells)s)
							AND od.dest_cell = ANY(%(dest_cells)s)
							AND od.interval = %(interval)s
					   )
SELECT routeLazy(cellpath_flow.cellpath) AS links,
	   flow AS flow 
FROM cellpath_flow

/*
WITH loaded_links AS (
	WITH cellpath_flow AS (	SELECT od.orig_cell, od.dest_cell, cellpath, cellpath.share * flow AS flow
							FROM od, getTopCellpaths(od.orig_cell, od.dest_cell, %(max_cellpaths)s) cellpath
							WHERE od.orig_cell = ANY(%(orig_cells)s)
								AND od.dest_cell = ANY(%(dest_cells)s)
								AND od.interval = %(interval)s
						   )
	SELECT 	od.orig_cell, 
			od.dest_cell, 
			routeLazy(cellpath_flow.cellpath) AS links,
		   	flow AS flow
	FROM cellpath_flow
)
SELECT links, flow FROM loaded_links
GROUP BY od.orig_cell, od.dest_cell
*/