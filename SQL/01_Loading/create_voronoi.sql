--Delete voronoi related objects
DROP INDEX IF EXISTS voronoi_gist;
DROP TABLE IF EXISTS public.voronoi CASCADE;

--Create table voronoi
CREATE TABLE public.voronoi
(
  id numeric(10,0),
  geom geometry(Polygon,4326),
  CONSTRAINT voronoi_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

COMMENT ON TABLE public.voronoi IS 
'Contains the trips with their respective cellpath used for route estimation';

COMMENT ON COLUMN public.voronoi.id IS 
'the antenna id that the voronoi cell belongs to';

COMMENT ON COLUMN public.voronoi.geom IS 
'a polygon describing the area of the Voronoi cell in WGS 84/Google (SRID 4326) projection';

CREATE INDEX voronoi_gist
  ON public.voronoi
  USING gist
  (geom);
