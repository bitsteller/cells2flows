SELECT EXTRACT(HOUR FROM trips.start_time)*6 + EXTRACT(MINUTE FROM trips.start_time)/10 AS interval, EXTRACT(HOUR FROM trips.start_time) AS hour, EXTRACT(MINUTE FROM trips.start_time) AS minute, COUNT(*)
FROM trips
WHERE date_part('dow', trips.start_time) BETWEEN 1 AND 4 
	AND EXTRACT(EPOCH FROM (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (extract(epoch from end_time) - 3600*distance/50) * INTERVAL '1 second') - start_time)/60 < 60
GROUP BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time)
ORDER BY EXTRACT(HOUR FROM trips.start_time), EXTRACT(MINUTE FROM trips.start_time)