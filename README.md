geo599-bigdata-hmt-project
==========================

### Objective
Map Highest Measured Tide in Oregon using LiDAR data (class project)

Basically this processing chain takes the VDatum grids, a highest measured tide incriment
above MHHW (as a grid or single value), and NAVD88 LiDAR data as input then does some magic (resampling, math, raster to vector conversions) and spits out a
vector dataset representing areas that fall under estimate of Highest Measured Tide.

This project is by no means opperational or reliable. It was produced for a class project
and may be incorporated into future work. You may be able to adapt some of the code to your project
if it relies on OGR / GDAL's python bindings.

Everything is released under the [MIT License](http://www.opensource.org/licenses/mit-license.php) but I ask that you credit me if you use it.


### Dependencies
*   GDAL / OGR (http://www.gdal.org/, and their python bindings)
*   Shapely (https://github.com/sgillies/shapely) (not implimented yet)
*   Parallel Python (http://www.parallelpython.com/) (if you want to run multiple LiDAR datasets at once)


### TODO
*   merge vector output together and dissolve
*   fully impliment parallel processing within the chain
*   some way to represent the results of a sensativity analysis / confidence intervals as part of the mapped HMT vector output
*   confirm the assumption that invdist interpolation of VDatum nodata regions is approriate
*   find a better way to model tidal variability along the Oregon Coast given the lack of NAVD88-referenced tidal stations
*   build a GUI / web interface to query, monitor progress, and export restults (similar to OpenTopography)


### References
*   NOAA Tides and Currents - http://tidesandcurrents.noaa.gov/
*   NOAA Tidal Datums - http://tidesandcurrents.noaa.gov/datum_options.html
*   Oregon DSL "Using Tidal Data for HMT" - http://www.oregon.gov/dsl/PERMITS/docs/using_tidal_data_for_hmt.doc
*   "Heads of Tide for Coastal Streams in Oregon" (OR DSL 1989) - http://www.oregon.gov/dsl/PERMITS/docs/heads_of_tide_1989.pdf?ga=t
*   NOAA VDatum - http://vdatum.noaa.gov/
*   Oregon Coast LiDAR Data - http://www.oregongeology.org/sub/projects/olc/