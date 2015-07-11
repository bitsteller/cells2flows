--Create table
DROP TABLE IF EXISTS hh_2po_4pgr_lite CASCADE;

CREATE TABLE hh_2po_4pgr_lite
(
  id integer NOT NULL,
  source integer,
  target integer,
  cost double precision,
  reverse_cost double precision,
  geom_way geometry(LineString,4326),
  CONSTRAINT pkey_hh_2po_4pgr_lite PRIMARY KEY (id)
);

--index for edge geometries
CREATE INDEX hh_2po_4pgr_lite_geom_way_idx
  ON hh_2po_4pgr_lite
  USING GIST (geom_way);

--Add border links to simplified network 
INSERT INTO hh_2po_4pgr_lite
SELECT DISTINCT r.id, r.source, r.target, r.cost, r.reverse_cost, r.geom_way
FROM boundary_edges AS e,
     hh_2po_4pgr AS r
WHERE r.id = e.edge_id
      /* Prevent duplicates */
      AND NOT EXISTS (SELECT id FROM hh_2po_4pgr_lite WHERE id = e.edge_id);