--closest_junction(a,b) searches the closest border junction to voronoi cell a in b
CREATE OR REPLACE FUNCTION closest_junction(integer, integer) RETURNS integer AS $$
SELECT a.junction_id
FROM boundary_junctions AS a, hh_2po_4pgr_vertices AS vertices, ant_pos AS b
WHERE a.antenna_id = $2 AND b.id = $1 AND a.junction_id = vertices.id
ORDER BY ST_DISTANCE(vertices.geom, b.geom) ASC
LIMIT 1 
$$ LANGUAGE SQL STABLE;


--select * from closest_junction(220,245)