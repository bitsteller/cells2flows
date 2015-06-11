--Drop exisiting antenna related objects
DROP INDEX IF EXISTS sidx_taz_geom;
DROP TABLE IF EXISTS public.taz CASCADE;

--Create taz table and index
CREATE TABLE public.taz
(
  taz_id integer NOT NULL,
  geom geometry(Polygon,4326) NOT NULL,
  CONSTRAINT taz_id_pkey PRIMARY KEY (taz_id)
);

COMMENT ON TABLE public.taz IS 
'Traffic analysis zones (TAZ) used as origin and desination zones in the OD matrix';

COMMENT ON COLUMN public.taz.taz_id IS 
'Unique identifier of the TAZ. Must match with IDs used in the OD table';

COMMENT ON COLUMN public.taz.geom IS 
'a polygon describing the area covered by the TAZ';

CREATE INDEX sidx_taz_geom
  ON public.taz
  USING gist
  (geom);