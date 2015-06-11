--Drop exisiting od related objects
DROP INDEX IF EXISTS idx_od_interval;
DROP TABLE IF EXISTS public.od CASCADE;

--Create od table and index
CREATE TABLE public.od
(
  origin_taz integer NOT NULL,
  destination_taz integer NOT NULL,
  interval integer,
  flow integer NOT NULL,
  CONSTRAINT od_pkey PRIMARY KEY (origin_taz, destination_taz)
);

COMMENT ON TABLE public.od IS 
'OD matrix storing the flows between each pair of TAZ zones.
Interval is the hour of the day (0-23) for which the flow is valid.';

COMMENT ON COLUMN public.od.origin_taz IS 
'taz_id of the origin TAZ';

COMMENT ON COLUMN public.od.destination_taz IS 
'taz_id of the destination TAZ';

COMMENT ON COLUMN public.od.interval IS 
'hour of the day (0-23) for which the flow is valid';

COMMENT ON COLUMN public.od.flow IS 
'number of vehicles from origin_taz to destination_taz during interval';

CREATE INDEX idx_od_interval
  ON public.od
  USING btree
  (interval);