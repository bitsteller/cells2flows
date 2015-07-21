--_assignCellpathSegmentids(cellpath) assigns segment ids to each cell in the cellpath based on the simplified cellpath
CREATE OR REPLACE FUNCTION _assignCellpathSegmentIds(integer[]) RETURNS TABLE(segment_id integer, cellid integer) AS
$BODY$
  DECLARE
    simplified integer[];
    BEGIN
    simplified := (SELECT simple_cellpath FROM simple_cellpath WHERE simple_cellpath.cellpath = $1);
    segment_id := 0;
    FOR i IN 1 .. array_length($1,1) LOOP
      cellid := $1[i];
      RETURN NEXT;
      IF simplified[segment_id+2] = cellid AND segment_id+2 < array_length(simplified,1) AND i > 1 AND i < array_length($1,1) THEN
        segment_id := segment_id + 1;
        RETURN NEXT;
      END IF;
    END LOOP;
    RETURN;
    END
$BODY$
LANGUAGE 'plpgsql'
STABLE
RETURNS NULL ON NULL INPUT;


--getCellpathSegments(cellpath) returns all major segments of a cellpath with segment id and an array of cells in the segment
CREATE OR REPLACE FUNCTION getCellpathSegments(integer []) RETURNS TABLE(segment_id integer, segment integer[]) AS $$
  SELECT segment_id, array_agg(cellid) 
  FROM _assignCellpathSegmentIds($1)
  GROUP BY segment_id
  ORDER BY segment_id
$$ LANGUAGE SQL STABLE;


--Testing:
--SELECT * FROM _assignCellpathSegmentIds(ARRAY[63,60,61,62,222,361,358,223,359,225,230,356,381,351,362,349,362,33,28,350,27,388,0,23,22,17,302,295,303,297,306])
--SELECT * FROM getCellpathSegments(ARRAY[63,60,61,62,222,361,358,223,359,225,230,356,381,351,362,349,362,33,28,350,27,388,0,23,22,17,302,295,303,297,306])

--view containing all segments for each cellpath
DROP MATERIALIZED VIEW IF EXISTS public.cellpath_segment;

CREATE MATERIALIZED VIEW public.cellpath_segment AS
  SELECT simple_cellpath.cellpath AS cellpath, segments.segment_id, segments.segment 
  FROM simple_cellpath, getCellpathSegments(simple_cellpath.cellpath) AS segments 
  ORDER BY simple_cellpath.cellpath, segment_id
WITH DATA;

COMMENT ON MATERIALIZED VIEW public.cellpath_segment IS 
'Contains a partition into segments for each cellpath. 
For each cellpath the table contains segments of the cellpath enumerated with a ascending segment id.
Concatenating all segments yields the original cellpath. 
Splitting into segments is done using line simplfiication of the original cellpath.';

COMMENT ON COLUMN public.cellpath_segment.cellpath IS
'original cellpath (array of cellids)';

COMMENT ON COLUMN public.cellpath_segment.segment_id IS
'the id of the segment (starting at 0)';

COMMENT ON COLUMN public.cellpath_segment.segment IS
'the cellpath segment (array of cellids)';

CREATE INDEX cellpath_segment_cellpath_segment_id_idx
  ON public.cellpath_segment
  USING btree
  (cellpath, segment_id);

