--creates a view containing the cellpath distribution for every cell OD pair 
--(the probabilities for the different cellpaths between a pair of origin and destination cells)

DROP MATERIALIZED VIEW IF EXISTS cellpath_dist CASCADE;

CREATE MATERIALIZED VIEW cellpath_dist AS 
  SELECT  trips.start_antenna, 
          trips.end_antenna, 
          cellpath, 
          COUNT(*)::double precision/(SELECT COUNT(*) FROM trips WHERE array_length(cellpath,1) >= 3) AS share
  FROM trips
  WHERE array_length(cellpath,1) >= 3
  GROUP BY trips.start_antenna, trips.end_antenna, cellpath
WITH DATA;