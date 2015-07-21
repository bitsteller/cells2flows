--creates a view containing the cellpath distribution for every cell OD pair 
--(the probabilities for the different cellpaths between a pair of origin and destination cells)

DROP MATERIALIZED VIEW IF EXISTS cellpath_dist CASCADE;

CREATE MATERIALIZED VIEW cellpath_dist AS 
  ( -- "real" cellpaths
    SELECT  trips.start_antenna AS orig_cell, --origin cell id
            trips.end_antenna AS dest_cell, --destination cell id
            cellpath, --cellpath array containg the list of visitied cells
            COUNT(*)::double precision/(SELECT COUNT(*) 
                                        FROM trips AS trips_all 
                                        WHERE trips_all.start_antenna = trips.start_antenna 
                                          AND trips_all.end_antenna = trips.end_antenna 
                                          AND array_length(trips_all.cellpath,1) >= 2) 
              AS share --probablity of the cellpath for the given cell od pair
    FROM trips
    WHERE array_length(cellpath,1) >= 2
    GROUP BY trips.start_antenna, trips.end_antenna, cellpath
  )
  UNION
  ( --virtual cellpaths for OD pairs without real cellpaths
    SELECT  od.orig_cell AS orig_cell,
            od.dest_cell AS dest_cell,
            ARRAY[od.orig_cell, od.dest_cell] AS cellpath,
            1.0::double precision AS share
    FROM od
    WHERE NOT EXISTS(SELECT * FROM trips WHERE trips.start_antenna = od.orig_cell AND trips.end_antenna = od.dest_cell)  
  )
WITH DATA;

CREATE INDEX idx_cellpath_dist_orig
  ON public.cellpath_dist
  USING btree
  (orig_cell);

CREATE INDEX idx_cellpath_dist_dest
  ON public.cellpath_dist
  USING btree
  (dest_cell);