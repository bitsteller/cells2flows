--This function returns all triplets of cells in a cellpath
CREATE OR REPLACE FUNCTION getParts(integer[]) RETURNS TABLE(part integer[]) AS
$BODY$
    BEGIN
    FOR i IN 1 .. array_length($1,1)-2
    LOOP
	part := ARRAY[$1[i], $1[i+1], $1[i+2]];
	RETURN NEXT;
    END LOOP;
    RETURN;
    END
$BODY$
    LANGUAGE 'plpgsql'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

--select parts.* from trips_cellpath, getParts(trips_cellpath.cellpath) as parts
--order by id