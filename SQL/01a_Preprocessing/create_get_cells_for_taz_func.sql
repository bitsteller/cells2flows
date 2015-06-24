-- get_cells_for_taz(taz_id) searches for all cells that intersect with the taz
-- returns a table containing all intersecting cells and their share of the area of the TAZ that they cover
-- the sum of all shares adds up to 1.0

CREATE OR REPLACE FUNCTION get_cells_for_taz(integer) RETURNS TABLE(cellid numeric(10,0), share double precision) AS $$
BEGIN
  RETURN QUERY 
  WITH taz AS (SELECT *, ST_AREA(taz.geom) AS area FROM taz WHERE taz_id = $1),
     cells AS (SELECT voronoi.id AS id, voronoi.geom AS geom FROM voronoi, taz WHERE ST_Intersects(voronoi.geom, taz.geom))
  SELECT cell.id AS cellid, ST_AREA(ST_INTERSECTION(taz.geom, cell.geom))/taz.area AS share
  FROM cells AS cell, taz;
  RETURN;
END
$$ LANGUAGE plpgsql STABLE;


--cache cell mapping in materialized view for performance
DROP MATERIALIZED VIEW IF EXISTS taz_cells CASCADE;

CREATE MATERIALIZED VIEW taz_cells AS 
 SELECT taz.taz_id,
    get_cells_for_taz.cellid AS cell_id,
    get_cells_for_taz.share
   FROM taz,
    LATERAL get_cells_for_taz(taz.taz_id) get_cells_for_taz(cellid, share)
WITH DATA;

CREATE INDEX idx_taz_cells_taz_id
  ON public.taz_cells
  USING btree
  (taz_id);

--SELECT get_cells_for_taz(22062000)