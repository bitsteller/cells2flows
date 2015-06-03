SELECT trips.start_antenna, trips.end_antenna, cellpath, COUNT(*)::double precision/(SELECT COUNT(*) FROM trips_cellpath WHERE trips_cellpath.start_antenna = trips.start_antenna AND trips_cellpath.end_antenna = trips.end_antenna AND array_length(cellpath,1) >= 3) AS share
FROM trips_cellpath AS trips
WHERE array_length(cellpath,1) >= 3
GROUP BY trips.start_antenna, trips.end_antenna, cellpath
