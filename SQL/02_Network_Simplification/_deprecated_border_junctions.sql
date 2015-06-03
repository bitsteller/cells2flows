-- create view border_edges
CREATE OR REPLACE VIEW border_edges AS
SELECT v.id AS antenna_id,
       r.id AS edge_id
FROM voronoi AS v,
     hh_2po_4pgr AS r
WHERE ST_Intersects(ST_ExteriorRing(v.geom), r.geom_way);

-- create table border_junction
CREATE TABLE IF NOT EXISTS public.border_junctions
(
  antenna_id integer NOT NULL,
  junction_id integer NOT NULL,
  CONSTRAINT border_junctions_pkey PRIMARY KEY (antenna_id, junction_id)
);

-- populate table border_junction
WITH homeless_border_junctions AS (
       SELECT DISTINCT j_1.id,
          j_1.geom
         FROM hh_2po_4pgr_vertices j_1,
          hh_2po_4pgr r,
          border_edges e
        WHERE (j_1.id = r.source OR j_1.id = r.target) AND r.id = e.road_id
      )
INSERT INTO border_junctions
SELECT v.id AS antenna_id,
  j.id AS junction_id
 FROM voronoi v,
  homeless_border_junctions j
WHERE st_contains(v.geom, j.geom);