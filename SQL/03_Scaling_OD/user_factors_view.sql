DELETE MATERIALIZED VIEW IF EXISTS cell_factors;
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