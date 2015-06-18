-- UPSERTs an od flow
-- adds %(flow)s to the cell od flow between %(orig_cell)s and %(dest_cell)s for %(interval)s
-- if a row for the given od pair does not yet exist, a new row is created (UPSERT)

WITH new_values (o, d, i, flow) as (
  values 
     (%(orig_cell)s, %(dest_cell)s, %(interval)s::int, %(flow)s)
),
upsert as
( 
    update od m 
        set flow = m.flow + nv.flow
    FROM new_values nv
    WHERE m.orig_cell = nv.o AND m.dest_cell = nv.d AND ((m.interval IS NULL AND nv.i IS NULL) OR m.interval = nv.i)
    RETURNING m.*
)
INSERT INTO od (orig_cell, dest_cell, interval, flow)
SELECT o, d, i, flow
FROM new_values
WHERE NOT EXISTS (SELECT 1 
                  FROM od up 
                  WHERE up.orig_cell = new_values.o AND up.dest_cell = new_values.d AND ((new_values.i IS NULL AND up.interval IS NULL) OR up.interval = new_values.i))