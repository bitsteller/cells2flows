-- replaces %(oldcell)s in every trip cellpath with %(newcell)s and 
-- also updates start_antenna and end_antenna columns if necessary

WITH trips_to_update AS (SELECT * FROM trips WHERE %(oldcell)s = ANY(trips.cellpath))
UPDATE trips SET (start_antenna, end_antenna, cellpath) = (updated_trips.start_antenna, updated_trips.end_antenna, updated_trips.cellpath)
FROM (  SELECT id, 
	CASE WHEN trips_to_update.start_antenna = %(oldcell)s THEN %(newcell)s ELSE trips_to_update.start_antenna END AS start_antenna,
	CASE WHEN trips_to_update.end_antenna = %(oldcell)s THEN %(newcell)s ELSE trips_to_update.end_antenna END AS end_antenna,
	array_replace(trips_to_update.cellpath, %(oldcell)s, %(newcell)s) AS cellpath
			   FROM trips_to_update) AS updated_trips
WHERE trips.id = updated_trips.id