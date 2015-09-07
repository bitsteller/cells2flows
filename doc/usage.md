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

## Loading parameters
- `ANTENNA_FILE`: filname of the csv file containing the antenna positions
- `TRIPS_FILE`: filname of the csv file containing the trips (STEM data/cellpaths)
- `TAZ_FILE`: filname of the geojson file containing the TAZ geometries
- `OD_FILE`: filename of the pickle file containing the OD flows for pairs of TAZs

## Computation parameters
- `MIN_ANTENNA_DIST`: min distance between antennas in meteres (antennas with a smaller distance are merged during clustering)
- `MAX_CELLPATHS`: the maxium number of cellpaths to use for each OD pair, only the `MAX_CELLPATHS` most likeley cellpaths (larger value = more accurate, smaller value=faster computation); recommended value: 10
- `ROUTE_ALGORITHM`: route algorithm to use during network loading, supportet algorithms are "LAZY" (Lazy Voronoi Routing), "STRICT" (Strict Voronoi Routing), "SHORTEST" (shortest path between start and destination cell)
- `ALGO_PARAMS`: algorithm specific paramters given as a dictionary (are passed to the flows.sql query of the chosen algorithm). The `LAZY` algorithm needs the following paramters to be set: `alpha` (cost factor for visited cells), `beta` (cost factor for buffer around visited cells), `extdist` (buffer distance in meters). Example how to set `ALGO_PARAMS`:

		ALGO_PARAMS = {"alpha": "0.01", "beta": "1.0", "extdist": 500}
	
- `CELLS` (optional):  a sequence of cell ids to use for the computation, be careful that after antenna clustering ids might change. If the this paramter is omitted, all cells are used by default.
- `TRIPS` (optional): trip ids to use, if not set all loaded trips are used

# Additional parameters

Observer that when running the full experiment, you need to adjust the additional parameters in the beginning of the following files:

	* `trip_scaling.py` (population)
	*  `od.py` (e.g. trip-time distribution related parameters and weekdays to consider)

See the comments in these files for more info about each parameter.

# Run the full traffic assignment procedure
	
When you performed all the previous steps , you can run each experiment through:

	python run_experiment.py $experiment
	
The following experiments are available:

	* `sstem`: Runs a network loading from SSTEM data. The hh_2po_4pgr table has to be existent and filled with data. All remaining data is read from the files given in the configuration and loaded into the database. The given OD is converted into a cell based matrix and then the cellpath routing and network loading components are used to calculate the link flows.
	* `routevalidation`: Validates 1000 randomly selected routes based on SSTEM data. Additionally routes from MATSim are read and compared with the routes estimated from the SSTEM data. The result is presented as diagrams and printed on console.
	* `full`: Runs the full procedure except the trip extraction. The hh_2po_4pgr, trips and ant_pos tables have to be existent and filled with data already. Runs the OD matrix estimation, cellpath routing and network loading components to calcualte the link flows.
	
*Warning*: Be aware that when you run the process from the beginning all previously calculated data will be deleted from the database.
	
# Run tests
To run unit tests on the the util.py module:
	
	python -m unittest test_util
		
After run_experiments.py has finished with all steps, you can verify the (intermediate) results in the database. To run fast tests (some test only check a random sample of the data) use 

	python -m unittest test_db_integrity_fast
	
To check all data you can additionally run the slow tests (this can take several hours depending on the amout of data):

	python -m unittest test_db_integrity_slow
	
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


