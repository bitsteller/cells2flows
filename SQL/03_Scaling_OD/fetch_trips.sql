--fetches the trip data necessary for OD estimation
-- %(weekdays)s array of weekdays to consider trips from (0=sunday,...6=saturday)
-- %(speed)s average travel speed (km/h) assumed in order to calculate the latest possible start time of the trip

SELECT 	start_antenna,
		end_antenna,
		trip_scale_factor,
		EXTRACT(EPOCH FROM start_time) AS start,
		EXTRACT(EPOCH FROM end_time) AS end,
		EXTRACT(EPOCH FROM end_time) - 3600*distance/%(speed)s AS start_interval_end
FROM trips_with_factors
WHERE start_antenna = ANY(%(orig_cells)s) AND end_antenna = ANY(%(dest_cells)s)
AND EXTRACT(DOW FROM start_time) = ANY(%(weekdays)s)