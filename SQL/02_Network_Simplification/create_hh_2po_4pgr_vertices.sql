--Drop exisiting antenna related objects
DROP INDEX IF EXISTS hh_2po_4pgr_vertices_geom_idx;
DROP TABLE IF EXISTS public.hh_2po_4pgr_vertices CASCADE;
DROP SEQUENCE IF EXISTS public.hh_2po_4pgr_vertices_id_seq;

--Create ant_pos table and index
CREATE SEQUENCE public.hh_2po_4pgr_vertices_id_seq
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;

CREATE TABLE public.hh_2po_4pgr_vertices
(
  id integer NOT NULL DEFAULT nextval('hh_2po_4pgr_vertices_id_seq'::regclass),
  geom geometry(Geometry,4326),
  CONSTRAINT hh_2po_4pgr_vertices_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX hh_2po_4pgr_vertices_geom_idx
  ON public.hh_2po_4pgr_vertices
  USING btree
  (geom);

--Insert intersections from hh_2po_4pgr
INSERT INTO hh_2po_4pgr_vertices (id, geom)
WITH p AS ((SELECT source AS id, x1 AS x, y1 AS y FROM hh_2po_4pgr) UNION (SELECT target AS id, x2 AS x, y2 AS y FROM hh_2po_4pgr))
SELECT id, ST_SetSRID(ST_Makepoint(x, y),4326) FROM p

--also create index for edges
CREATE INDEX hh_2po_4pgr_geom_way_idx
  ON hh_2po_4pgr
  USING GIST (geom_way);