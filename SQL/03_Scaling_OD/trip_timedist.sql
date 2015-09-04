--fetches the trip-time distribute of all well-defined trips
-- %(weekdays)s array of weekdays to consider trips from (0=sunday,...6=saturday)
-- %(speed)s average travel speed (km/h) assumed in order to calculate the latest possible start time of the trip
-- %(maxinterval)s the maximum duration of the trip start interval (minutes) to consider the trip well-defined 
SELECT EXTRACT(HOUR FROM trips.start_time)*6 + EXTRACT(MINUTE FROM trips.start_time)/10 AS interval, COUNT(*) AS count
FROM trips
WHERE date_part('DOW', trips.start_time) = ANY(%(weekdays)s)
AND EXTRACT('EPOCH' FROM (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (EXTRACT('EPOCH' FROM end_time) - 3600*distance/%(speed)s) * INTERVAL '1 second') - start_time)/60 < %(maxinterval)s
GROUP BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time)
ORDER BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time)