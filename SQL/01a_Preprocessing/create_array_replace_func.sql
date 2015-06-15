--called as array_replace(original_array, oldelement, newelement)
--replaces every oldelement in original_array with another newelement at returns the result as a new array
CREATE OR REPLACE FUNCTION array_replace(anyarray, anyelement, anyelement) RETURNS timestamp
    AS $$ SELECT array_agg(newarr) FROM (SELECT CASE WHEN el = $1 THEN $2 ELSE el END 
					  FROM unnest($1) as el) newarr; 
    $$ LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

--SELECT array_replace(cellpath, 10, 2) FROM trips LIMIT 1 --test array_replace function



