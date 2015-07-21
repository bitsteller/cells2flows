DROP MATERIALIZED VIEW IF EXISTS voronoi_extended;

CREATE MATERIALIZED VIEW voronoi_extended AS
	SELECT id, ST_Transform(ST_Buffer(ST_Transform(voronoi.geom,3857), 500),4326) geom
	FROM voronoi
WITH DATA;

--routeSegmentLazy(start_junction, end_junction, segment) for an array of cellids (segment) 
-- calculates a route from start_junction (hh_2po_4pgr_vertices.id) to end_junction preferring links inside the segment cells
-- returns a setof linkids (hh_2po_4pgr.id)
CREATE OR REPLACE FUNCTION routeSegmentLazy(integer, integer, integer[]) RETURNS SETOF integer AS $$
	BEGIN
 	RETURN QUERY (
 	SELECT r.id2
 	FROM pgr_dijkstra('	WITH preferred_links AS 
 							(SELECT hh_2po_4pgr_lite.id
 							 FROM public.hh_2po_4pgr_lite, voronoi
 						 	WHERE voronoi.id = ANY(ARRAY[' || array_to_string($3,',') || '])
 						 		AND ST_Intersects(hh_2po_4pgr_lite.geom_way, voronoi.geom)),
 							preferrable_links AS
 							(SELECT hh_2po_4pgr_lite.id
 							 FROM public.hh_2po_4pgr_lite, voronoi_extended
 						 	WHERE voronoi_extended.id = ANY(ARRAY[' || array_to_string($3,',') || '])
 						 		AND ST_Intersects(hh_2po_4pgr_lite.geom_way, voronoi_extended.geom))
 						SELECT hh_2po_4pgr_lite.id, 
 							source, 
 							target, 
 							(CASE WHEN EXISTS(SELECT * FROM preferred_links WHERE preferred_links.id = hh_2po_4pgr_lite.id) THEN
 								0.5*cost
 							WHEN EXISTS(SELECT * FROM preferrable_links WHERE preferrable_links.id = hh_2po_4pgr_lite.id) THEN
 								0.8*cost
 							ELSE
 								cost
 							END) AS cost,
 							(CASE WHEN EXISTS(SELECT * FROM preferred_links WHERE preferred_links.id = hh_2po_4pgr_lite.id) THEN
 								0.5*reverse_cost
 							WHEN EXISTS(SELECT * FROM preferrable_links WHERE preferrable_links.id = hh_2po_4pgr_lite.id) THEN
 								0.8*reverse_cost
 							ELSE
 								reverse_cost
 							END) AS reverse_cost --,
							--ST_X(ST_StartPoint(hh_2po_4pgr_lite.geom_way)) AS x1,
							--ST_Y(ST_StartPoint(hh_2po_4pgr_lite.geom_way)) AS y1,
							--ST_X(ST_EndPoint(hh_2po_4pgr_lite.geom_way)) AS x2,
							--ST_Y(ST_EndPoint(hh_2po_4pgr_lite.geom_way)) AS y2
 						FROM hh_2po_4pgr_lite', $1, $2, true, true) AS r
 	WHERE r.id2 <> -1);
	EXCEPTION WHEN others THEN
		RETURN QUERY (SELECT NULL::integer AS id2);
    END
$$ LANGUAGE plpgsql STABLE; --TODO: parameterize SRID


--test:
--SELECT routeSegmentLazy(best_startpoint(ARRAY[0,4]), best_endpoint(ARRAY[8,196]),ARRAY[0,4,0,4,186,4,316,185,215,346,385,357,219,220,331,207,365,8,196])


--routeLazy(cellpath) for an array of cellids (cellpath) 
-- calculates a route roughly following the cellpath (uses characteristic cells as 
-- waypoints and modified cost on links inside visited cells)
CREATE OR REPLACE FUNCTION routeLazy(integer[]) RETURNS integer[] AS $$
	SELECT array_agg(linkid) FROM
		(WITH via AS (SELECT array_append(array_prepend((SELECT junction_id FROM get_candidate_junctions($1[array_lower($1,1)]) ORDER BY random() LIMIT 1),--best_startpoint($1), 
													   get_waypoints(scp.simple_cellpath)), 
													   (SELECT junction_id FROM get_candidate_junctions($1[array_upper($1,1)]) ORDER BY random() LIMIT 1)--best_endpoint($1)
										 ) AS points
					 FROM simple_cellpath AS scp
					 WHERE scp.cellpath = $1),
		segment AS (SELECT segment_id, segment 
					FROM cellpath_segment
					WHERE cellpath_segment.cellpath = $1
					ORDER BY segment_id ASC)
		SELECT DISTINCT linkid
		FROM segment, via, LATERAL routeSegmentLazy(via.points[segment_id+1], via.points[segment_id+2], segment.segment) AS linkid)
	AS links;
$$ LANGUAGE SQL STABLE;


										 ) AS points
					 FROM simple_cellpath AS scp
					 WHERE scp.cellpath = $1),
		segment AS (SELECT segment_id, segment 
					FROM cellpath_segment
					WHERE cellpath_segment.cellpath = $1
					ORDER BY segment_id ASC)
		SELECT via.points[segment_id+1], via.points[segment_id+2], segment.segment
		FROM segment, via
$$ LANGUAGE SQL STABLE;

