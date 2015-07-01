-- STRICT Voronoi routing algorithm
-- assigns flows to the road network from all %(orig_cells)s to all %(dest_cells)s during interval %(interval)s
-- using the top %(max_cellpaths)s cellpaths per OD pair
-- returns a result set containing a flow and an array of link ids (hh_2po_4pgr.id) that the flow should be allocated to
-- 
-- STRICT Voronoi routing uses a waypoint in each cell in the cellpath to force the traffic to enter the Voronoi cell

WITH ranked_cellpath AS (
	SELECT cellpath_dist.*, ROW_NUMBER() OVER(PARTITION BY (cellpath_dist.orig_cell,cellpath_dist.dest_cell) ORDER BY cellpath_dist.share DESC) AS rank
	FROM cellpath_dist
	WHERE cellpath_dist.orig_cell = ANY(%(orig_cells)s)
		AND cellpath_dist.dest_cell = ANY(%(dest_cells)s)
	)

SELECT cellpath_dist.share * od.flow AS flow,
	(route_with_waypoints(array_append(array_prepend(best_startpoint(cellpath_dist.cellpath), get_waypoints(cellpath_dist.cellpath)), best_endpoint(cellpath_dist.cellpath)))).edges AS links
FROM (SELECT orig_cell, dest_cell, cellpath, share FROM ranked_cellpath WHERE rank <= %(max_cellpaths)s) AS cellpath_dist, od
WHERE cellpath_dist.orig_cell = od.orig_cell
AND cellpath_dist.dest_cell = od.dest_cell
AND od.interval = %(interval)s