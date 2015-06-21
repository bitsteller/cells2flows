--create route type containging edges and a route cost
DROP TYPE IF EXISTS route CASCADE;
CREATE TYPE route AS (
    edges   integer[],
    cost    double precision
);


-- route_with_waypoints(array_of_waypoints) calculates a route that passes through all intersections in the given waypoint array.
-- The result is a route object containg the used link ids and a cost. 
-- If no route was found, the link list is empty and the cost infinite. 
CREATE OR REPLACE FUNCTION route_with_waypoints(integer[]) RETURNS route AS
$BODY$
    DECLARE
    	part_route route;
	temp_edges integer[] := ARRAY[]::integer[];
	temp_cost double precision := 0.0;
    BEGIN
    FOR i IN 1 .. array_length($1,1)-1
    LOOP
	part_route := (SELECT ROW(array_agg(r.id2), SUM(r.cost)) FROM pgr_dijkstra('SELECT * from astar_links_lite',$1[i],$1[i+1],false, false) AS r);
	temp_edges := array_cat(temp_edges, part_route.edges[1:array_length(part_route.edges,1)-1]);
	temp_cost := temp_cost + part_route.cost;
    END LOOP;

    RETURN (temp_edges,temp_cost)::route;
    EXCEPTION WHEN others THEN
	RETURN (ARRAY[]::integer[], 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.0)::route;
    END
$BODY$
    LANGUAGE 'plpgsql'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

--Test route_with_waypoints: select * from route_with_waypoints(ARRAY[2,100,10])

-- best_startpoint(cellpath) where cellpath is a list of travelled cells
-- calculates the best starting point (hh_2po_4pgr_vertices.id) in the first cell in the path when heading to the second cell
CREATE OR REPLACE FUNCTION best_startpoint(integer[]) RETURNS int AS $$
WITH possible_waypoints AS (    SELECT * 
                FROM boundary_junctions 
                WHERE antenna_id = $1[1] 
                ORDER BY ST_DISTANCE((SELECT geom FROM hh_2po_4pgr_vertices AS node WHERE node.id = closest_junction($1[1], $1[2])), (SELECT geom FROM hh_2po_4pgr_vertices AS node WHERE node.id = boundary_junctions.junction_id))
                LIMIT 5
                )
SELECT waypoint.junction_id
FROM possible_waypoints waypoint, route_with_waypoints(ARRAY [waypoint.junction_id, closest_junction($1[1], $1[2])]) route
ORDER BY route.cost
LIMIT 1
$$ LANGUAGE SQL STABLE;


-- best_endpoint(cellpath) where cellpath is a list of travelled cells
-- calculates the best starting point (hh_2po_4pgr_vertices.id) in the last cell of the path when coming from the second last cell
CREATE OR REPLACE FUNCTION best_endpoint(integer[]) RETURNS int AS $$
WITH possible_waypoints AS (    SELECT * 
                FROM boundary_junctions 
                WHERE antenna_id = $1[array_length($1, 1)]
                ORDER BY ST_DISTANCE((SELECT geom FROM hh_2po_4pgr_vertices AS node WHERE node.id = closest_junction($1[array_length($1, 1)], $1[array_length($1, 1)-1])), (SELECT geom FROM hh_2po_4pgr_vertices AS node WHERE node.id = boundary_junctions.junction_id))
                LIMIT 5
                )
SELECT waypoint.junction_id
FROM possible_waypoints waypoint, route_with_waypoints(ARRAY [closest_junction($1[array_length($1, 1)], $1[array_length($1, 1)-1]), waypoint.junction_id]) route
ORDER BY route.cost
LIMIT 1
$$ LANGUAGE SQL STABLE;

-- get_waypoints(cellpath) returns an array of waypoints for the given cellpath by lookup in the waypoints table
-- All 3-segments are extracted from the cellpath each of which a waypoint is looked up in the waypoint table
CREATE OR REPLACE FUNCTION get_waypoints(integer[]) RETURNS int[] AS $$
WITH parts AS (SELECT getParts($1) AS part)
SELECT array_cat(ARRAY []::integer[],
    (SELECT array_agg(
        (SELECT waypoint 
        FROM waypoints 
        WHERE waypoints.part = parts.part))
    FROM parts))
$$ LANGUAGE SQL STABLE;


--Test:
--SELECT trips.cellpath, array_append(array_prepend(best_startpoint(trips.cellpath), get_waypoints(trips.cellpath)),best_endpoint(trips.cellpath)) AS waypoints, 
--    (route_with_waypoints(array_append(array_prepend(best_startpoint(trips.cellpath), get_waypoints(trips.cellpath)),best_endpoint(trips.cellpath)))).edges AS links
--FROM trips_cellpath AS trips
--LIMIT 10
