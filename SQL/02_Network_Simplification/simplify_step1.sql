--Create table
DROP TABLE IF EXISTS hh_2po_4pgr_lite CASCADE;

CREATE TABLE hh_2po_4pgr_lite
(
  id integer NOT NULL,
  source integer,
  target integer,
  cost double precision,
  CONSTRAINT pkey_hh_2po_4pgr_lite PRIMARY KEY (id)
);

--Add border links to simplified network 
INSERT INTO hh_2po_4pgr_lite
SELECT DISTINCT r.id, r.source, r.target, r.cost
FROM boundary_edges AS e,
     hh_2po_4pgr AS r
WHERE r.id = e.edge_id
      /* Prevent duplicates */
      AND NOT EXISTS (SELECT id FROM hh_2po_4pgr_lite WHERE id = e.edge_id);

--Debug view to view the simplified network in GIS
DROP VIEW IF EXISTS simple_roads;

CREATE OR REPLACE VIEW simple_roads AS 
 SELECT hh_2po_4pgr.id,
    hh_2po_4pgr.geom_way AS geom
   FROM hh_2po_4pgr,
    hh_2po_4pgr_lite
  WHERE hh_2po_4pgr.id = hh_2po_4pgr_lite.id;