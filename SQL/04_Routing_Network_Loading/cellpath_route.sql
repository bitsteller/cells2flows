CREATE OR REPLACE FUNCTION get_waypoints(integer[]) RETURNS int[] AS $$
WITH parts AS (SELECT getParts($1) AS part)
SELECT array_cat(ARRAY []::integer[],
	(SELECT array_agg(
		(SELECT waypoint 
		FROM waypoints 
		WHERE waypoints.part = parts.part))
	FROM parts))
$$ LANGUAGE SQL STABLE;


SELECT trips.cellpath, array_append(array_prepend(best_startpoint(trips.cellpath), get_waypoints(trips.cellpath)),best_endpoint(trips.cellpath)) AS waypoints, 
	(route_with_waypoints(array_append(array_prepend(best_startpoint(trips.cellpath), get_waypoints(trips.cellpath)),best_endpoint(trips.cellpath)))).edges AS links
FROM trips_cellpath AS trips
LIMIT 10
