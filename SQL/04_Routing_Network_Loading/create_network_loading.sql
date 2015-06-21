--Drop exisiting network loading related objects
DROP TABLE IF EXISTS public.network_loading CASCADE;

--Create waypoint table
CREATE TABLE public.network_loading
(
  id integer NOT NULL, -- the link id (hh_2po_4pgr.id) that the flow applies to
  interval integer, -- the interval that the flow is valid for, all intervals if not set
  flow double precision, -- the flow (number of vehicles) estimated for the given link
  CONSTRAINT network_loading_pkey PRIMARY KEY (id, interval)
);

COMMENT ON TABLE public.network_loading IS 
'Contains the main result, the estimated flows an the transportation network links. To view the result in GIS tools,
use the loaded_links view which adds a geom column to the table containing the link geometries.';


-- create loaded links view
DROP VIEW IF EXISTS public.loaded_links;

CREATE OR REPLACE VIEW public.loaded_links AS
 WITH nload AS (
         SELECT network_loading.id,
            network_loading.flow,
            network_loading.interval
           FROM network_loading
          WHERE network_loading.interval IS NULL
        )
 SELECT hh_2po_4pgr.id,
    hh_2po_4pgr.geom_way,
        CASE
            WHEN nload.flow IS NULL THEN 0::double precision
            ELSE nload.flow
        END AS flow
   FROM hh_2po_4pgr
     LEFT JOIN nload ON hh_2po_4pgr.id = nload.id;

COMMENT ON VIEW public.loaded_links IS 
'Contains the network loading result including the link geometries to view the result in GIS tools. To view a certain interval, 
change the line "WHERE network_loading.interval IS NULL" to "WHERE network_loading.interval = $desired_interval".
'