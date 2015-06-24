--Drop exisiting cellpath_segmented related objects
DROP TABLE IF EXISTS public.cellpath_segments CASCADE;

--Create cellpath_segments table
CREATE TABLE public.cellpath_segments
(
  cellpath integer[] NOT NULL, -- a cellpath (an array of cellids)
  segment_id integer NOT NULL, -- segment id
  segment integer[] NOT NULL, -- the segment of the original cellpath (an array of cellids)
  CONSTRAINT cellpath_segments_pkey PRIMARY KEY (cellpath, segment_id)
);

COMMENT ON TABLE public.cellpath_segments IS 
'Contains a partition into segments for each cellpath. 
For each cellpath the table contains segments of the cellpath enumerated with a segment id starting from 0.
Concatenating all segments yields the original cellpath. 
Splitting into segments is done using line simplfiication of the original cellpath.';