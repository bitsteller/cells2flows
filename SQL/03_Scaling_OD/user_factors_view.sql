DROP MATERIALIZED VIEW IF EXISTS cell_factors;
CREATE MATERIALIZED VIEW cell_factors AS
(
 SELECT homebase.user_id,
    cell_factors.day,
    cell_factors.trip_scale_factor
   FROM homebase,
    cell_factors
  WHERE homebase.antenna_id = cell_factors.antenna_id
  ORDER BY homebase.user_id
) WITH DATA;

COMMENT ON MATERIALIZED VIEW cell_factors IS
"Assumes that users inactive over the course of a day and people not in the data have the same behaviour as the sample of active users (those that made a trip on that day). 

active_share(cell, day) = users_with_trip/homebase_count
trip_scale_factor(cell, day) = 1/active_share * cell_population/homebase_count
trip_scale_factor(user, day) = trip_scale_factor(user_homebase, day)

Uses cell_factors, homebase.";