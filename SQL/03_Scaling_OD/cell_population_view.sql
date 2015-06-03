 SELECT homebase.antenna_id,
    homebase.no_users,
    13508715::bigint * homebase.no_users / total_homebase.total AS population
   FROM user_home_count homebase,
    ( SELECT sum(user_home_count.no_users) AS total
           FROM user_home_count) total_homebase;