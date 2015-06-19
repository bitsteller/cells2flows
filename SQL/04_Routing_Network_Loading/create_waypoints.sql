--Drop exisiting waypoint related objects
DROP TABLE IF EXISTS public.waypoint CASCADE;

--Create waypoint table
CREATE TABLE public.waypoint
(
  part integer[] NOT NULL, -- a cellpath segment consisting (an array of 3 cellids)
  waypoint integer, -- the best waypoint (hh_2po_4pgr_vertices.id) for the given cellpath segment
  CONSTRAINT waypoint_pkey PRIMARY KEY (part)
);

COMMENT ON TABLE public.waypoint IS 
'Waypoint table containing the best waypoint for each cellpath segment of three subsequent cells (a,b,c). 
The waypoint is selected inside cell b such that the the shortest path from a to c via b is minimized.';