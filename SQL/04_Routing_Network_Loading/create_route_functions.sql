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
	part_route := (SELECT ROW(array_agg(r.id2), SUM(r.cost)) FROM pgr_dijkstra('SELECT * FROM hh_2po_4pgr_lite',$1[i],$1[i+1], true, true) AS r);
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
SELECT startpoint
FROM best_startpoint
WHERE part = $1[1:2]
$$ LANGUAGE SQL STABLE;


-- best_endpoint(cellpath) where cellpath is a list of travelled cells
-- calculates the best starting point (hh_2po_4pgr_vertices.id) in the last cell of the path when coming from the second last cell
CREATE OR REPLACE FUNCTION best_endpoint(integer[]) RETURNS int AS $$
SELECT endpoint
FROM best_endpoint
WHERE part = $1[array_upper($1,1)-1:array_upper($1,1)]
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


-- _getTopCellpaths(orig_cellid, dest_cellid, n) fetches the top n (most likeley) cellpaths from orig_cellid to dest_cellid
-- the result is returned as a table containg the top n cellpath and their share (likelihood)
-- shares are normalized to sum up to 1.0 in the resulting table
CREATE OR REPLACE FUNCTION _getTopCellpaths(integer, integer, integer) RETURNS table(cellpath int[], share double precision) AS $$
    WITH ranked_cellpath AS (
        SELECT cellpath_dist.cellpath AS cellpath, cellpath_dist.share
        FROM cellpath_dist
        WHERE cellpath_dist.orig_cell = $1
            AND cellpath_dist.dest_cell = $2
        ORDER BY cellpath_dist.share DESC
        LIMIT $3
        )
    SELECT cellpath, share/(SELECT SUM(share) FROM ranked_cellpath)  FROM ranked_cellpath
$$ LANGUAGE SQL STABLE;


-- getTopCellpaths(orig_cellid, dest_cellid, n) fetches the top n (most likeley) cellpaths from orig_cellid to dest_cellid
-- the result is returned as a table containg the top n cellpath and their share (likelihood)
-- shares are normalized to sum up to 1.0 in the resulting table
-- if no cellpath is found, 
CREATE OR REPLACE FUNCTION getTopCellpaths(integer, integer, integer) RETURNS table(cellpath int[], share double precision) AS $$
    BEGIN
        IF EXISTS(SELECT * FROM _getTopCellpaths($1,$2,$3)) THEN
            RETURN QUERY (SELECT * FROM _getTopCellpaths($1,$2,$3));
        ELSE
            RETURN QUERY (SELECT ARRAY[$1,$2],1.0::double precision); --return virtual cellpath containing start and end cell
        END IF;
    END
$$ LANGUAGE 'plpgsql' STABLE;


--SELECT * FROM getTopCellpaths(1,4,5)

