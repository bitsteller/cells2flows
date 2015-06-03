--Add shortest paths to simplified network
--Parameter %(cell)s has to be replaced by a cell id before running
WITH targets AS (
         SELECT antenna_id, array_agg(junction_id) AS junctions
         FROM boundary_junctions
         WHERE antenna_id = %(cell)s
         GROUP BY antenna_id
         ),

     visited_edges AS (
         SELECT DISTINCT unnest(
             (SELECT array_agg(DISTINCT cr.id3) AS id FROM pgr_kdijkstraPath(
                 'SELECT id, source, target, cost FROM hh_2po_4pgr',
                 source.junction_id,
                 array_remove(targets.junctions, source.junction_id),
                 false,
                 false) AS cr)
              ) AS id
         FROM boundary_junctions AS source,
              targets
         WHERE source.antenna_id = targets.antenna_id
               AND array_length(targets.junctions, 1) > 1
		AND source.antenna_id = %(cell)s
)

INSERT INTO hh_2po_4pgr_lite
SELECT r.id, r.source, r.target, r.cost
FROM visited_edges AS e,
     hh_2po_4pgr AS r
WHERE r.id = e.id
      /* Prevent duplicates */
      AND NOT EXISTS (SELECT id FROM hh_2po_4pgr_lite WHERE id = e.id);