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
CREATE OR REPLACE FUNCTION routeSegmentLazy(integer, integer, integer[]) RETURNS integer[] AS $$
	SELECT array_agg(r.id2)
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