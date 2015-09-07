DROP MATERIALIZED VIEW IF EXISTS trips_with_factors CASCADE;
CREATE MATERIALIZED VIEW trips_with_factors AS
(
 SELECT trips.user_id,
    trips.start_antenna,
    trips.end_antenna,
    trips.start_time,
    trips.end_time,
    trips.distance,
    trips.the_geom,
    trips.start_arr,
    trips.end_arr,
    trips.id,
    trips.cellpath,
    trip_factors.trip_id,
    trip_factors.trip_scale_factor
   FROM trips,
    trip_factors
  WHERE trips.id = trip_factors.trip_id
) WITH DATA;
