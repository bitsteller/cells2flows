--Drop exisiting od related objects
DROP INDEX IF EXISTS idx_od_interval;
DROP TABLE IF EXISTS public.od CASCADE;

--Create od table and index
CREATE TABLE public.od
(
  orig_taz integer NOT NULL,
  dest_taz integer NOT NULL,
  interval integer,
  flow double precision NOT NULL,
  CONSTRAINT od_pkey PRIMARY KEY (orig_cell, dest_cell)
);

COMMENT ON TABLE public.od IS 
'OD matrix storing the flows between each pair of cells.
Interval is the hour of the day (0-23) for which the flow is valid.';

COMMENT ON COLUMN public.od.orig_cell IS 
'cell id of the origin cell';

COMMENT ON COLUMN public.od.dest_cell IS 
'cell id of the destination cell';

COMMENT ON COLUMN public.od.interval IS 
'hour of the day (0-23) for which the flow is valid';

COMMENT ON COLUMN public.od.flow IS 
'number of vehicles from orig_cell to dest_cell during interval';

CREATE INDEX idx_od_interval
  ON public.od
  USING btree
  (interval);