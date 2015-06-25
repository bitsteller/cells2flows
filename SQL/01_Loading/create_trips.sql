--Drop exisiting antenna related objects
DROP INDEX IF EXISTS idx_trips_start_antenna;
DROP INDEX IF EXISTS idx_trips_end_antenna;
DROP INDEX IF EXISTS idx_trips_start_end_antenna;
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

COMMENT ON TABLE public.trips IS 
'Contains the trips with their respective cellpath used for route estimation';

COMMENT ON COLUMN public.trips.id IS 
'a unique identifier of the trip';

COMMENT ON COLUMN public.trips.user_id IS 
'the user id of the user that the trip belongs to';

COMMENT ON COLUMN public.trips.start_antenna IS 
'The antenna id that the trip started from. Matches with the first cell id in the cellpath.';

COMMENT ON COLUMN public.trips.end_antenna IS 
'The antenna id that the trip ended in. Matches with the last cell id in the cellpath.';

COMMENT ON COLUMN public.trips.start_time IS 
'the anticipated time the trip started at';

COMMENT ON COLUMN public.trips.end_time IS 
'the anticipated time the trip ended at';

COMMENT ON COLUMN public.trips.distance IS 
'the total distance of the trip based on the straight line distance between all consecutive antennas in the cellpath';

COMMENT ON COLUMN public.trips.cellpath IS 
'an array of cell ids visited along the trip, in their order of occurance during the travel';

CREATE INDEX idx_trips_start_antenna
  ON public.trips
  USING btree
  (start_antenna);

CREATE INDEX idx_trips_end_antenna
  ON public.trips
  USING btree
  (end_antenna);

CREATE INDEX idx_trips_start_end_antenna
  ON public.trips
  USING btree
  (start_antenna, end_antenna);

--create a debug view containg lines connecting the cell centroids in every trip
CREATE OR REPLACE VIEW trips_geom AS 
 SELECT trips.id,
    ( SELECT st_makeline(( SELECT ant_pos.geom
                   FROM ant_pos
                  WHERE ant_pos.id = cellid.cellid)) AS st_makeline
           FROM unnest(trips.cellpath) cellid(cellid)) AS geom
   FROM trips;