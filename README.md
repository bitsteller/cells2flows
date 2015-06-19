# cells2flows
Estimates traffic flows from cellular network data.

## Purpose

This script aims to estimate traffic flow in a transportation network from cellular network data (the cells that users visited).
The input OD flows are converted into cell OD flows between the antenna cells. Routes are estimated based on 
the cells that users visited during their travel with a "lazy Voronoi routing" algorithm. 
Special attention in the code has been paid to efficiency, parallization and memory-usage to be able to handle large-scale data.

## Input & Outputs

*Inputs*:
- transportation network
- cellular network data (user cellpaths)
- antenna positions
- OD matrix containing the travel demand between traffic analysis zones (TAZs)
- TAZ geometries

*Outputs*:
- network loading (a traffic flow for each link in the transportation network)

## Features
- supports arbitrary origin/destination geometries (TAZs) for OD matrix that can be different from the cell geometries
- performs the whole process by running a single script `run_experiment.py`
- configurable `MIN_ANTENNA_DISTANCE` parameter allows to reduce the computation time while impacting the result reasonably
- configurable bounding box allows to perform the computation for a certain area only
- data is stored in postgres database, allowing easy access through GIS tools like QGIS for viewing results
- low memory-consumption allows running on older machines
- fast computation due to paralellization of all computation-intensive steps
- status information / completion time estimation for all compuation intesive steps

## Setup & Usage

Please follow the instructions in doc/setup.md
