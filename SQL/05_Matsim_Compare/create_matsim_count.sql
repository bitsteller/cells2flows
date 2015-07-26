--aggregates the matsim link counts (linkids from matsim_shapefile)
DROP MATERIALIZED VIEW IF EXISTS matsim_count;
CREATE MATERIALIZED VIEW matsim_count AS (
  WITH links AS (
    SELECT linkid FROM (SELECT * FROM matsim) matsim, LATERAL unnest(linkpath) AS linkid
  )
  SELECT linkid, COUNT(linkid)
  FROM links
  GROUP BY linkid
);