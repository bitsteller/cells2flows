--Drop exisiting antenna related objects
DROP INDEX IF EXISTS idx_start_antenna;
DROP INDEX IF EXISTS idx_end_antenna;
DROP TABLE IF EXISTS public.trips CASCADE;
DROP SEQUENCE IF EXISTS public.trips_id_seq;

--Create ant_pos table and index
CREATE SEQUENCE public.trips_id_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 9223372036854775807
  START 0
  CACHE 1;

CREATE TABLE public.trips
(
  id integer NOT NULL DEFAULT nextval('trips_id_seq'::regclass),
  user_id integer,
  start_antenna integer,
  end_antenna integer,
  start_time timestamp,
  end_time timestamp,
  distance double precision,
  cellpath int[],
  CONSTRAINT trips_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX idx_trips_start_antenna
  ON public.trips
  USING btree
  (start_antenna);

CREATE INDEX idx_trips_end_antenna
  ON public.trips
  USING btree
  (end_antenna);