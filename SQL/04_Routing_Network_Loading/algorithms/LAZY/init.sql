--routeSegmentLazy(start_junction, end_junction, segment) for an array of cellids (segment) 
-- calculates a route from start_junction (hh_2po_4pgr_vertices.id) to end_junction preferring links inside the segment cells
CREATE OR REPLACE FUNCTION routeSegmentLazy(integer, integer, integer[]) RETURNS integer[] AS $$
	SELECT array_agg(r.id2)
 	FROM pgr_dijkstra('	WITH preferred_links AS 
 							(SELECT * FROM hh_2po_4pgr_lite
 						 	WHERE EXISTS(SELECT * FROM voronoi WHERE voronoi.id = ANY(ARRAY[' || array_to_string($3,',') || ']) AND ST_Intersects(voronoi.geom, hh_2po_4pgr_lite.geom_way)))
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