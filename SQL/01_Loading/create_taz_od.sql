--Drop exisiting od related objects
DROP INDEX IF EXISTS idx_taz_od_interval;
DROP TABLE IF EXISTS public.taz_od CASCADE;

--Create od table and index
CREATE TABLE public.taz_od
(
  origin_taz integer NOT NULL,
  destination_taz integer NOT NULL,
  interval integer,
  flow integer NOT NULL,
  CONSTRAINT taz_od_pkey PRIMARY KEY (origin_taz, destination_taz)
);

COMMENT ON TABLE public.taz_od IS 
'OD matrix storing the flows between each pair of TAZ zones.
Interval is the hour of the day (0-23) for which the flow is valid.';

COMMENT ON COLUMN public.taz_od.origin_taz IS 
'taz_id of the origin TAZ';

COMMENT ON COLUMN public.taz_od.destination_taz IS 
'taz_id of the destination TAZ';

COMMENT ON COLUMN public.taz_od.interval IS 
'hour of the day (0-23) for which the flow is valid';

COMMENT ON COLUMN public.taz_od.flow IS 
'number of vehicles from origin_taz to destination_taz during interval';

CREATE INDEX idx_taz_od_interval
  ON public.taz_od
  USING btree
  (interval);

-- debug view for vizualizing taz_od flows in GIS tools
CREATE OR REPLACE VIEW taz_od_geom AS
 SELECT row_number() OVER () AS id,
    taz_od.origin_taz,
    taz_od.destination_taz,
    taz_od."interval",
    taz_od.flow,
    st_makeline(ST_Centroid(o.geom), ST_Centroid(d.geom)) AS geom
   FROM taz_od,
    taz o,
    taz d
  WHERE taz_od.origin_taz = o.taz_id AND taz_od.destination_taz = d.taz_id AND o.taz_id <> d.taz_id;