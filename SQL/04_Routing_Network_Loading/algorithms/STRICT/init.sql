--routeStrict(cellpath) for an array of cellids (cellpath) 
-- calculates a route strictly following the cellpath (entering every cell in the cellpath)
-- returns an array of hh_2po_4pgr link ids
CREATE OR REPLACE FUNCTION routeStrict(integer[]) RETURNS integer[] AS $$
	SELECT (route_with_waypoints(array_append(array_prepend(	best_startpoint($1), 
															get_waypoints($1)), 
															best_endpoint($1)))).edges AS links;
$$ LANGUAGE SQL STABLE;

--make strict voronoi the default routing algorithm
CREATE OR REPLACE FUNCTION route(integer[]) RETURNS integer[] AS $$
	SELECT routeStrict($1) AS links;
$$ LANGUAGE SQL STABLE;