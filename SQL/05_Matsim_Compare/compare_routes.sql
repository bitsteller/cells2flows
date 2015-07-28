--creates the type containg the results of the comparison
DROP TYPE IF EXISTS route_compare_result CASCADE;
CREATE TYPE route_compare_result AS (
    ms_geom geometry(LineString,4326), --MatSim route geoemtry
    e_geom geometry(LineString,4326), -- estimated geometry
    similarity double precision, -- similarity (0..1) based on the share of common intersections
    common_points int, -- number of intersection that appear in both routes
    extra_ms_points int, -- number of intersections that only appear in the matsim route
    extra_e_points int, -- number of intersections that only appear in the estimated route
    total_points int -- total number of distinct intersections in both routes
);

--compareRoutes(matsim link ids, hh_2po_4pgr link ids) compares a matsim route to a route in the hh_2po_4pgr road network
--returns a route_compare_result containing a similarity score from (0=no similarity, 1=equality)
CREATE OR REPLACE FUNCTION compareRoutes(ms_linkpath integer[], e_linkpath integer[]) RETURNS route_compare_result AS
$BODY$
    DECLARE
    	r route_compare_result;
    	ms_points geometry(Point, 4326)[];
    	e_points geometry(Point, 4326)[];
    	tolerance double precision := 0.0001;
    BEGIN/*
    	r.ms_geom := (SELECT st_linemerge
    						 (
                 				( 	SELECT st_union(ST_Transform(ST_LineMerge(matsim_shapefile.geom), 4326)) AS st_union
                           			FROM unnest(ms_linkpath) linkid(linkid)
                             		JOIN matsim_shapefile ON matsim_shapefile.id::integer = linkid.linkid
                             	)
                     		 )
    				 );
    	r.e_geom := (WITH e_links AS (
                 		SELECT 	link.linkid,
                    			hh_2po_4pgr.geom_way AS geom
                   		FROM unnest(e_linkpath) link(linkid)
                     	JOIN hh_2po_4pgr ON hh_2po_4pgr.id = link.linkid
                	 )
         			 SELECT st_linemerge(( 	SELECT st_union(e_links.geom) AS st_union
                   	 						FROM e_links
                   	 					)) AS geom);
        */
    	ms_points := (  SELECT array_agg(ST_Transform(geom,4326))
                        FROM ((  SELECT DISTINCT ST_StartPoint(ST_LineMerge(matsim_shapefile.geom)) AS geom
                              	 FROM unnest(ms_linkpath) link(linkid)
                                 JOIN matsim_shapefile ON matsim_shapefile.id::integer = link.linkid
                             ) UNION
                             (   SELECT DISTINCT ST_EndPoint(ST_LineMerge(matsim_shapefile.geom)) AS geom
                              	 FROM unnest(ms_linkpath) link(linkid)
                                 JOIN matsim_shapefile ON matsim_shapefile.id::integer = link.linkid
                             )) AS p
                     );

    	e_points := (  SELECT array_agg(p.geom) AS st_union
                        FROM ((  SELECT DISTINCT ST_StartPoint(hh_2po_4pgr.geom_way) AS geom
                              	 FROM unnest(e_linkpath) link(linkid)
                                 JOIN hh_2po_4pgr ON hh_2po_4pgr.id = link.linkid
                             ) UNION
                             (   SELECT DISTINCT ST_EndPoint(hh_2po_4pgr.geom_way) AS geom
                              	 FROM unnest(e_linkpath) link(linkid)
                                 JOIN hh_2po_4pgr ON hh_2po_4pgr.id = link.linkid
                             )) AS p
                     );
    	
    	r.common_points := (  SELECT COUNT(*)
    						  FROM unnest(ms_points) ms_point(geom), unnest(e_points) e_point(geom)
    						  WHERE ST_DWithin(ms_point.geom, e_point.geom, tolerance)
    					   );

    	r.extra_ms_points := (  SELECT COUNT(*)
    						  	FROM unnest(ms_points) ms_point(geom)
    						  	WHERE NOT EXISTS (SELECT * 
    						  					  FROM unnest(e_points) e_point(geom)
    						  					  WHERE ST_DWithin(ms_point.geom, e_point.geom, tolerance)
    						  					 )
    					  	 );
    	r.extra_e_points :=  (  SELECT COUNT(*)
    						  	FROM unnest(e_points) e_point(geom)
    						  	WHERE NOT EXISTS (SELECT * 
    						  					  FROM unnest(ms_points) ms_point(geom)
    						  					  WHERE ST_DWithin(e_point.geom, ms_point.geom, tolerance)
    						  					 )
    					  	 );
    	r.total_points := r.common_points + r.extra_ms_points + r.extra_e_points;
    	r.similarity := r.common_points::double precision / r.total_points::double precision;
	RETURN r;
    END
$BODY$
    LANGUAGE 'plpgsql'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;