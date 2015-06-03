--Drop exisiting antenna related objects
DROP INDEX IF EXISTS sidx_ant_pos_geom;
DROP TABLE IF EXISTS public.ant_pos CASCADE;
DROP SEQUENCE IF EXISTS public.ant_pos_id_seq;

--Create ant_pos table and index
CREATE SEQUENCE public.ant_pos_id_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 9223372036854775807
  START 0
  CACHE 1;

CREATE TABLE public.ant_pos
(
  id integer NOT NULL DEFAULT nextval('ant_pos_id_seq'::regclass),
  lon real,
  lat real,
  geom geometry(Point,4326),
  CONSTRAINT ant_pos_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX sidx_ant_pos_geom
  ON public.ant_pos
  USING gist
  (geom);