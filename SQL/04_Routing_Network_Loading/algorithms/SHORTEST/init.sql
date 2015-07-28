--routeShortest(cellpath) for an array of cellids (cellpath) 
-- calculates a route following the shortest path between the first and last cell in the cellpath
-- returns an array of hh_2po_4pgr link ids
CREATE OR REPLACE FUNCTION routeStrict(integer[]) RETURNS integer[] AS $$
	SELECT (route_with_waypoints(array_append(array_prepend(best_startpoint($1), 
														ARRAY[]::int[]), 
														best_endpoint($1)))).edges AS links;
$$ LANGUAGE SQL STABLE;

--make shortest the default routing algorithm
CREATE OR REPLACE FUNCTION route(integer[]) RETURNS integer[] AS $$
	SELECT routeStrict($1) AS links;
$$ LANGUAGE SQL STABLE;