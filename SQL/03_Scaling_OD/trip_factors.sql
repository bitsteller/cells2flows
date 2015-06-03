﻿INSERT INTO trip_factors
 SELECT trips.id AS trip_id, user_factors.trip_scale_factor 
   FROM trips, user_factors
  WHERE trips.user_id = user_factors.user_id AND date_part('day'::text, trips.start_time) = date_part('day'::text, user_factors.day)
  ORDER BY trips.id
