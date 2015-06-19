--creates a view containing the cellpath parts necessary for waypoint calculation
--Only parts for cellpaths starting or ending in a cell in %(cells)s are included

DROP MATERIALIZED VIEW IF EXISTS cellpath_parts CASCADE;

CREATE MATERIALIZED VIEW cellpath_parts AS 
SELECT DISTINCT parts.part AS part
			FROM trips, getParts(trips.cellpath) AS parts
			WHERE NOT parts.part[1] = parts.part[2] AND NOT parts.part[3] = parts.part[2]
      			  AND start_antenna IN %(cells)s AND end_antenna IN %(cells)s
WITH DATA;