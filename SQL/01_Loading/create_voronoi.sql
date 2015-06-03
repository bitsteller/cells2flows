--Delete voronoi related objects
DROP INDEX IF EXISTS voronoi_gist;
DROP TABLE IF EXISTS public.voronoi CASCADE;

--Create table voronoi
CREATE TABLE public.voronoi
(
  id numeric(10,0),
  lon numeric,
  lat numeric,
  geom geometry(Polygon,4326),
  CONSTRAINT voronoi_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX voronoi_gist
  ON public.voronoi
  USING gist
  (geom);
