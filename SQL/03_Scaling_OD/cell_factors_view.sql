DROP MATERIALIZED VIEW IF EXISTS cell_factors;
CREATE MATERIALIZED VIEW cell_factors AS
(
 WITH days AS (SELECT generate_series(MIN(start_time), MAX(start_time), '1 day'::interval) AS day FROM trips)
 SELECT homebase.antenna_id,
    day.day,
    count(DISTINCT trips.user_id) AS active_users,
    user_home_count.no_users,
    count(DISTINCT trips.user_id)::double precision / user_home_count.no_users::double precision AS active_share,
    1::double precision / (count(DISTINCT trips.user_id)::double precision / user_home_count.no_users::double precision) * cell_population.population::double precision / user_home_count.no_users::double precision AS trip_scale_factor
   FROM trips,
    homebase,
    user_home_count,
    days,
    cell_population
  WHERE homebase.antenna_id = ANY(%(cells)s) AND homebase.user_id = trips.user_id AND homebase.antenna_id = user_home_count.antenna_id AND trips.start_time >= day.day AND trips.start_time <= (day.day + '1 day'::interval) AND cell_population.antenna_id = homebase.antenna_id
  GROUP BY homebase.antenna_id, user_home_count.no_users, day.day, cell_population.population
  ORDER BY homebase.antenna_id, day.day
) WITH DATA;