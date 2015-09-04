# Setup traffic assignment method

## Setup a local postgres database

1. Install postgresql, postgis, pgrouting
2. Init DB in an empty folder:

		initdb -D <path for DB storage>

3. Start DB server:

		postgres -D <path for DB storage>

4. Create the database (postgis template):

		createdb template_postgis

5. Load spatial data types and functions:

		psql template_postgis -f `pg_config —sharedir`/contrib/postgis-2.1/postgis.sql
		psql template_postgis -f `pg_config —sharedir`/contrib/postgis-2.1/spatial_ref_sys.sql

6. Create the extensions:

		echo “CREATE EXTENSION postgis;” | psql -d postgres
		echo “CREATE EXTENSION pgrouting;” | psql -d postgres
	
7. Create pl/python language:

		CREATE LANGUAGE plpythonu;

## Load OSM data for pgrouting into database

1. Download osm file (for example from http://download.geofabrik.de, https://mapzen.com/metro-extracts/).
2. Download osm2po (http://osm2po.de).
3. Convert the OSM data for the database using osm2po (replace los-angeles_california.osm.pbf with the actual name of the OSM file) with:

		java -Xmx512m -jar osm2po-core-5.0.0-signed.jar prefix=la los-angeles_california.osm.pbf

4. Insert the tables to the database:

		psql -d postgres -f la/la_2po_4pgr.sql 

5. As sanity check you can try if the following query works and returns a route (node ids work for Los Angeles):

		SELECT * FROM pgr_dijkstra(‘SELECT * from la_2po_4pgr’,2442,132209,false, false)

## Setup OSRM

1. Install OSRM (http://project-osrm.org)
2. Go into the folder where the OSM file is (that was previously used to load into the DB for pgrouting)
3. To prepare OSRM (build contraction hierarchy tree), run:

		osrm-extract los-angeles_california.osm.pbf
		osrm-prepare los-angeles_california.osrm

4. Run the server:
	
		osrm-routed los-angeles_california.osrm
	
Instead of running osrm as a server the pyosrm wrapper library can be used. In that case you need to make sure you need to run `make install` for osr m and make sure pyosrm is installed. You don't need to start `osrm-routed` in that case.
	
# Install python packages

Install all necessary python libraries using `pip` and the `requirements.txt` file inside the `/doc` folder:

	pip install -r requirements.txt
	
Installing the `keyring` library is optional.
	
# Adjust parsing functions

You will have to adjust the following parsing functions to be able to read your input file formats.

- parse_trajectory(linestr)
- parse_antenna(linestr)
- parse_trip(linestr)
- parse_taz(feature)

You find these functions including documentation on what they need to return, in the file `util.py`.

# Usage

You have now setup the method. Instructions about how to run the experiments can be found in the file `usage.md`.