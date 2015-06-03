-- create boundary_edges view
CREATE OR REPLACE VIEW boundary_edges AS

SELECT v.id AS antenna_id,
       r.id AS edge_id
FROM voronoi AS v,
     hh_2po_4pgr AS r
WHERE ST_Intersects(ST_ExteriorRing(v.geom), r.geom_way);

-- create boundary_junctions view
DROP MATERIALIZED VIEW IF EXISTS boundary_junctions CASCADE;

CREATE MATERIALIZED VIEW boundary_junctions AS 
 WITH homeless_boundary_junctions AS (
         SELECT DISTINCT j_1.id,
            j_1.geom
           FROM hh_2po_4pgr_vertices j_1,
            hh_2po_4pgr r,
            boundary_edges e
          WHERE (j_1.id = r.source OR j_1.id = r.target) AND r.id = e.edge_id
        )
 SELECT v.id AS antenna_id,
    j.id AS junction_id
   FROM voronoi v,
    homeless_boundary_junctions j
  WHERE st_contains(v.geom, j.geom)
WITH DATA;