--creates a view containing simplified cellpaths
DROP MATERIALIZED VIEW IF EXISTS simple_cellpath CASCADE;

CREATE MATERIALIZED VIEW simple_cellpath AS 
  WITH cellpath_geom AS
  (WITH cellpaths AS (SELECT DISTINCT cellpath FROM cellpath_dist)
  SELECT cellpaths.cellpath,
      ( SELECT st_makeline(( SELECT ant_pos.geom
                     FROM ant_pos
                    WHERE ant_pos.id = cellid.cellid)) AS st_makeline
             FROM unnest(cellpaths.cellpath) cellid(cellid)) AS geom,
     ( SELECT st_simplify(st_makeline(( SELECT ant_pos.geom
             FROM ant_pos
            WHERE ant_pos.id = cellid.cellid)),%(tolerance)s) AS st_simplify
     FROM unnest(cellpaths.cellpath) cellid(cellid)) AS simple_geom
     FROM cellpaths)
  SELECT  cellpath_geom.cellpath, 
          (SELECT array_agg(ant_pos.id) 
           FROM ST_DumpPoints(cellpath_geom.simple_geom) scpg
           JOIN ant_pos ON ant_pos.geom = scpg.geom) AS simple_cellpath
  FROM cellpath_geom
  GROUP BY cellpath_geom.cellpath, cellpath_geom.simple_geom
WITH DATA;

CREATE INDEX idx_simple_cellpath_cellpath
  ON public.simple_cellpath
  USING btree
  (cellpath);