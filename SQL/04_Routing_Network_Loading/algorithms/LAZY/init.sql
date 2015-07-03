-- create view that assigns a cell to every link in the road network (the cell that the link is inside or closest to)
DROP MATERIALIZED VIEW IF EXISTS public.hh_2po_4pgr_lite_with_cells;

CREATE MATERIALIZED VIEW public.hh_2po_4pgr_lite_with_cells AS
	WITH enclosing_cell AS (
		SELECT hh_2po_4pgr_lite.id AS linkid, voronoi.id AS cellid
		FROM hh_2po_4pgr_lite, voronoi
		WHERE ST_Within(geom_way, voronoi.geom)
		)
	SELECT 	id,
			source, 
			target, 
			cost, 
			geom_way, 
			(CASE WHEN EXISTS(SELECT * FROM enclosing_cell WHERE enclosing_cell.linkid = hh_2po_4pgr_lite.id) THEN
				(SELECT cellid FROM enclosing_cell WHERE enclosing_cell.linkid = hh_2po_4pgr_lite.id)
			ELSE
				(SELECT id FROM voronoi ORDER BY ST_Distance(ST_Centroid(voronoi.geom), ST_Centroid(geom_way)) ASC LIMIT 1)
			END) AS cellid
	FROM hh_2po_4pgr_lite
	ORDER BY hh_2po_4pgr_lite.id
WITH DATA;


--routeSegmentLazy(start_junction, end_junction, segment) for an array of cellids (segment) 
-- calculates a route from start_junction (hh_2po_4pgr_vertices.id) to end_junction preferring links inside the segment cells
-- returns a setof linkids (hh_2po_4pgr.id)
CREATE OR REPLACE FUNCTION routeSegmentLazy(integer, integer, integer[]) RETURNS SETOF integer AS $$
	SELECT r.id2
 	FROM pgr_dijkstra('	WITH preferred_links AS 
 							(SELECT * FROM public.hh_2po_4pgr_lite_with_cells
 						 	WHERE hh_2po_4pgr_lite_with_cells.cellid = ANY(ARRAY[' || array_to_string($3,',') || ']))
 						SELECT 	id, 
 							source, 
 							target, 
 							(CASE WHEN EXISTS(SELECT * FROM preferred_links WHERE preferred_links.id = hh_2po_4pgr_lite.id) THEN
 								0.5*cost
 							ELSE
 								cost
 							END) AS cost
 						FROM hh_2po_4pgr_lite',$1,$2,false, false) AS r;
$$ LANGUAGE SQL STABLE;


--test:
--SELECT routeSegmentLazy(best_startpoint(ARRAY[0,4]), best_endpoint(ARRAY[8,196]),ARRAY[0,4,0,4,186,4,316,185,215,346,385,357,219,220,331,207,365,8,196])


--routeLazy(cellpath) for an array of cellids (cellpath) 
-- calculates a route roughly following the cellpath (uses characteristic cells as 
-- waypoints and modified cost on links inside visited cells)
CREATE OR REPLACE FUNCTION routeLazy(integer[]) RETURNS integer[] AS $$
	WITH via AS (SELECT array_append(array_prepend(best_startpoint($1), 
												   get_waypoints(scp.simple_cellpath)), 
												   best_endpoint($1)) AS points
				 FROM simple_cellpath AS scp
				 WHERE scp.cellpath = $1),
	segment AS (SELECT segment_id, segment 
				FROM cellpath_segment
				WHERE cellpath_segment.cellpath = $1
				ORDER BY segment_id ASC)
	SELECT array_agg(routeSegmentLazy(via.points[segment_id], via.points[segment_id+1], segment.segment))
	FROM segment, via;
$$ LANGUAGE SQL STABLE;




