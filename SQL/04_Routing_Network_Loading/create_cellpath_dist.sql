--creates a view containing the cellpath distribution for every cell OD pair 
--(the probabilities for the different cellpaths between a pair of origin and destination cells)

DROP MATERIALIZED VIEW IF EXISTS cellpath_dist CASCADE;

CREATE MATERIALIZED VIEW cellpath_dist AS 
  SELECT  trips.start_antenna AS orig_cell, --origin cell id
          trips.end_antenna AS dest_cell, --destination cell id
          cellpath, --cellpath array containg the list of visitied cells
          COUNT(*)::double precision/(SELECT COUNT(*) 
                                      FROM trips AS trips_all 
                                      WHERE trips_all.start_antenna = trips.start_antenna 
                                        AND trips_all.end_antenna = trips.end_antenna 
                                        AND array_length(cellpath,1) >= 3) 
            AS share --probablity of the cellpath for the given cell od pair
  FROM trips
  WHERE array_length(cellpath,1) >= 3
  GROUP BY trips.start_antenna, trips.end_antenna, cellpath
WITH DATA;