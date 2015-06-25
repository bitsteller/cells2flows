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