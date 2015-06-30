--creates a view containing simplified cellpaths
DROP MATERIALIZED VIEW IF EXISTS simple_cellpath CASCADE;

CREATE MATERIALIZED VIEW simple_cellpath AS 
  WITH cellpath_geom AS
  (WITH cellpaths AS (SELECT DISTINCT cellpath FROM trips)
  SELECT cellpaths.cellpath,
      ( SELECT st_makeline(( SELECT ant_pos.geom
                     FROM ant_pos
                    WHERE ant_pos.id = cellid.cellid)) AS st_makeline
             FROM unnest(cellpaths.cellpath) cellid(cellid)) AS geom,
     ( SELECT st_simplify(st_makeline(( SELECT ant_pos.geom
             FROM ant_pos
            WHERE ant_pos.id = cellid.cellid)),0.05) AS st_simplify
     FROM unnest(cellpaths.cellpath) cellid(cellid)) AS simple_geom
     FROM cellpaths)
  SELECT  cellpath_geom.cellpath, 
      array_agg((SELECT ant_pos.id FROM ant_pos WHERE ant_pos.geom = simplified.geom)) AS simple_cellpath
  FROM cellpath_geom, ST_DumpPoints(cellpath_geom.simple_geom) simplified
  GROUP BY cellpath_geom.cellpath
WITH DATA;

CREATE INDEX idx_simple_cellpath_cellpath
  ON public.simple_cellpath
  USING btree
  (cellpath);


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
    IF $1[i] = simplified[segment_id+1] AND i < array_length($1,1) THEN
      IF i > 1 THEN
        RETURN NEXT;
      END IF;
      segment_id := segment_id + 1;
      RETURN NEXT;
    ELSE
      RETURN NEXT;
    END IF;
    END LOOP;
    RETURN;
    END
$BODY$
LANGUAGE 'plpgsql'
IMMUTABLE
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