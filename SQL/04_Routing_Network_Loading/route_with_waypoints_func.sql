--CREATE TYPE route AS (
--    edges   integer[],
--    cost    double precision
--);

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

select * from route_with_waypoints(ARRAY[2,100,10])