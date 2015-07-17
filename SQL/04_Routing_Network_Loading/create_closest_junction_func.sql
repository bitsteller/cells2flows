--closest_junction(a,b) searches the closest border junction to voronoi cell a in b
--if there are no intersections in b, a nearby intersection is returned
CREATE OR REPLACE FUNCTION closest_junction(integer, integer) RETURNS integer AS $$
	BEGIN
        IF EXISTS (SELECT * FROM boundary_junctions WHERE antenna_id = $2) THEN
            RETURN (SELECT a.junction_id
					FROM boundary_junctions AS a, hh_2po_4pgr_vertices AS vertices, ant_pos AS b
					WHERE a.antenna_id = $2 AND b.id = $1 AND a.junction_id = vertices.id
					ORDER BY ST_DISTANCE(vertices.geom, b.geom) ASC
					LIMIT 1); --return the closest junction to a in b
        ELSE
            RETURN (SELECT a.junction_id
					FROM boundary_junctions AS a, hh_2po_4pgr_vertices AS vertex, ant_pos
					WHERE 
						ant_pos.id = $2 AND a.junction_id = vertex.id
					ORDER BY ST_DISTANCE(vertex.geom, ant_pos.geom) ASC
					LIMIT 1); -- return the junction closest to b (better than nothing)
        END IF;
    END
$$ LANGUAGE 'plpgsql' STABLE;


--select * from closest_junction(220,245)

--get_candidate_junctions(cellid) returns all boundary junctions of cellid or 
--if there are no vertices in cellid it returns the 5 junctions closest to cellid
CREATE OR REPLACE FUNCTION get_candidate_junctions(integer) RETURNS table(junction_id integer, lat double precision, lon double precision) AS $$
	BEGIN
        IF EXISTS (SELECT * FROM boundary_junctions WHERE antenna_id = $1) THEN
            RETURN QUERY (	SELECT bj.junction_id AS junction_id, ST_Y(vertex.geom) AS lat, ST_X(vertex.geom) AS lon
							FROM boundary_junctions AS bj, hh_2po_4pgr_vertices AS vertex
							WHERE bj.antenna_id = $1 AND bj.junction_id = vertex.id); -- return the border junctions of cellid
        ELSE
            RETURN QUERY (SELECT a.junction_id, ST_Y(vertex.geom) AS lat, ST_X(vertex.geom) AS lon
					FROM boundary_junctions AS a, hh_2po_4pgr_vertices AS vertex, ant_pos
					WHERE
						ant_pos.id = $1 AND a.junction_id = vertex.id
					ORDER BY ST_DISTANCE(vertex.geom, ant_pos.geom) ASC
					LIMIT 10); --return 10 nearby junctions if there are no border junctions of cellid
        END IF;
    END
$$ LANGUAGE 'plpgsql' STABLE;


--select * from closest_junction(220,245)