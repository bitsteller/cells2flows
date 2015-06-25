--Drop exisiting waypoint related objects
DROP TABLE IF EXISTS public.best_startpoint CASCADE;

--Create waypoint table
CREATE TABLE public.best_startpoint
(
  part integer[] NOT NULL, -- a cellpath part consisting of the first 2 cellids in the cellpath in an array
  startpoint integer, -- the best startpoint (hh_2po_4pgr_vertices.id) for cellpaths starting with the given part
  CONSTRAINT best_startpoint_pkey PRIMARY KEY (part)
);

COMMENT ON TABLE public.best_startpoint IS 
'Contains the best startpoint for each cellpath starting part of the two first subsequent cells (a,b). 
The startpoint is selected inside cell a such that the the shortest path from a to b is minimized.';

--Drop exisiting waypoint related objects
DROP TABLE IF EXISTS public.best_endpoint CASCADE;

--Create waypoint table
CREATE TABLE public.best_endpoint
(
  part integer[] NOT NULL, -- a cellpath part consisting of the last 2 cellids in the cellpath in an array
  endpoint integer, -- the best endpoint (hh_2po_4pgr_vertices.id) for cellpaths starting with the given part
  CONSTRAINT best_endpoint_pkey PRIMARY KEY (part)
);

COMMENT ON TABLE public.best_endpoint IS 
'Contains the best endpoint for each cellpath starting part of the two last subsequent cells (a,b). 
The startpoint is selected inside cell b such that the the shortest path from a to b is minimized.';