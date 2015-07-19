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

Install the following python libraries using pip:

	pip install $name_of_the_library
	
* numpy
* scipy
* matplotlib
* urllib2
* psycopg2
* geopy
* keyring (optional)
	
# Adjust parsing functions

You will have to adjust the following parsing functions to be able to read your input file formats.

- parse_trajectory(linestr)
- parse_antenna(linestr)
- parse_trip(linestr)
- parse_taz(feature)

You find these functions including documentation on what they need to return, in the file `util.py`.

# Adjust configuration parameters

All configuration parameters are set in the file `config.py`. The available parameters are:

## Database credentials & general settings

- `DATABASE`: name of the postgres database to connect to
- `USER`: database username to use
- `PORT`: port to use for the database connection
- `PASSWORD` (optional): password for the databse connection. If you don't won't to save the password in cleartext, you can leave this parameter out and you will be promptet during runtime with the option to save the password in the system keychain instead
- `PYOSRM` (optional): If False (default), osrm has to listen as an HTTP server on localhost to answer routing request; if True the pyosrm wrapper library is used to communicate with osrm (much faster)
- `PYOSRM_FILE` (necessary if PYOSRM=TRUE): The prepared .osrm file that osrm should use for routing
- `NOTIFY_CMD` (optional): a command that will be called upon completion or failure of the traffic assignment, a message is passed to the command through STDIN. The script could for example send a push notification to www.boxcar.io or send an email to notify you of the computation status
- 
## Loading parameters
- `ANTENNA_FILE`: filname of the csv file containing the antenna positions
- `TRIPS_FILE`: filname of the csv file containing the trips (STEM data/cellpaths)
- `TAZ_FILE`: filname of the geojson file containing the TAZ geometries
- `OD_FILE`: filename of the pickle file containing the OD flows for pairs of TAZs

## Computation parameters
- `MIN_ANTENNA_DIST`: min distance between antennas in meteres (antennas with a smaller distance are merged during clustering)
- `MAX_CELLPATHS`: the maxium number of cellpaths to use for each OD pair, only the `MAX_CELLPATHS` most likeley cellpaths (larger value = more accurate, smaller value=faster computation); recommended value: 10
- `ROUTE_ALGORITHM`: route algorithm to use during network loading, supportet algorithms are "LAZY" (Lazy Voronoi Routing), "STRICT" (Strict Voronoi Routing), "SHORTEST" (shortest path between start and destination cell)
- `CELLS` (optional):  a sequence of cell ids to use for the computation, be careful that after antenna clustering ids might change. If the this paramter is omitted, all cells are used by default.
- `TRIPS` (optional): trip ids to use, if not set all loaded trips are used

# Run the full traffic assignment procedure
	
When you performed all the previous steps , you can run the whole pipleline through:

	python run_experiment.py
	
*Warning*: Be aware that when you run the process from the beginning all previously calculated data will be deleted from the database.
	
# DB Structure and Tables

## Input tables:
* hh_2po_4pgr: road network links (loaded through osm2po)
* ant_pos: antenna positions (loaded from csv)
* trips: dataset of user trip cellpaths (loaded from csv)
* taz_od: OD matrix containing flows betwen TAZs (loaded from pickle file)
* taz: Traffic analysis zone (TAZ) polygon geometries (loaded from geojson file)

## Intermediate tables:
* od: cell OD matrix containg the flows between cells (converted from taz_od)
* hh_2po_4pgr_vertices: intersection table
* waypoints: best waypoints for cellpath segments
* voronoi: Voronoi polygon for each antenna

## Intermediate views:
* boundary_junctions
* cellpath_parts
* boundary_edges
* cellpath_dist

## Output tables/views:
* network_loading
* loaded_links

	interval: either hour (0-23) or 10min intervals (0-143)


