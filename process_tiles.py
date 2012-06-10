#!/usr/bin/env python

"""
Copyright (c) 2012 Michael Ewald, Geomatics Research. <michael.ewald@geomaticsresearch.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

"""

# Import core modules
import sys
import os
import pprint

# Import and configure logging
import logging
logger = logging.getLogger('hmt_processor')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
ch.setFormatter(formatter)
logger.addHandler(ch)
fhnd = logging.FileHandler('logs/hmt_processor.log')
fhnd.setLevel(logging.DEBUG)
fhnd.setFormatter(formatter)
logger.addHandler(fhnd)

# Import Parallel Python
import pp

# Import Numpy
import numpy as np

# Import GDAL et al.
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

# Import Geomatics Research helpers
from gmtools import filesystem as gm_fs

# Import HMT specific packages
from hmt_processor import processors as hmt
from hmt_processor import hmt_gdal

# Fix osgeo error reporting
gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

# Set gdal configuration parameters
gdal.SetCacheMax(2147483648)  # 2 GB. This sets the caching max size for gdal. Bigger number can lead to faster reads / writes
gdal.SetConfigOption('HFA_USE_RRD', 'YES')  # Configure GDAL to use blocks

# get a reference to the path that holds this file
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the LIDAR datasets
LIDAR_DIR = os.path.join(PROJECT_DIR, 'data', 'LIDAR')

# Path to the MHHW datasets
TIDALDATUMS_DIR = os.path.join(PROJECT_DIR, 'data', 'tidal_datums')

# Path to the Tidal Incriment datasets
TIDALINCRIMENT_DIR = os.path.join(PROJECT_DIR, 'data', 'hmt_incriment')

# File folders that break up LIDAR tiles
SITE_BLOCKS = ['Neh_LIDAR', 'SSNERR_LIDAR', 'Till_LIDAR']


def tile_job(raw_quad_path, hmt_value, processed_quad_path):
  pass

def data_processor(name, data_block, lidar_quads, small=False, parallel=False):
  """ Process data for the estuary """
  
  logger.info("Welcome to the {0} data processor!".format(name))
  
  # Limit the number of tiles to two if we don't want to do the full run
  if small is True:
    lidar_quads = lidar_quads[0:1]
    logger.warn("Restricting lidar quads to the first item in list")
  
  # Lists to holds filepaths
  lidar_paths_full = list()
  output_quads = list()
  output_shps = list()
  if parallel is True:
    # Set up the parallel python server
    ppserver = pp.Server(secret="TheEagleHasLanded")
    ppserver.set_ncpus(1)  # Set the number of CPUs to 1, comment out to autodetect
    logger.info("Using {0} CPUs".format(ppserver.get_ncpus()))
    logger.info("Active PP nodes: {0}".format(ppserver.get_active_nodes()))
  
    ppjobs = list()
    for quad in lidar_quads:
      job = ppserver.submit(test_job, (quad,))
      ppjobs.append((quad, job))
    # Done adding jobs
    
    # wait for jobs to complete
    ppserver.wait()  
    
    # Print out the stats from parallel python
    logger.info("Parallel Python Stats")
    job_stats = ppserver.get_stats()  # get stats as dict
    for src in job_stats.keys():
      logger.info("  {0} - {1}: {2}".format(src, 'ncpus', job_stats[src].ncpus))
      logger.info("  {0} - {1}: {2}".format(src, 'njobs', job_stats[src].njobs))
      logger.info("  {0} - {1}: {2}".format(src, 'rworker', job_stats[src].rworker))
      logger.info("  {0} - {1}: {2}".format(src, 'time', job_stats[src].time))
    
    # Get results from the job
    for quad, jobresult in ppjobs:
      output = jobresult()
      print output
  
    #ppserver.print_stats()  # print stats
  
    # close out of Parallel Python server
    ppserver.destroy()
    sys.exit(1)
  
  # Loop through each quad, this could be easily parallelized
  for quad in lidar_quads:
    logger.info("Working on quad: {0}".format(quad))
    
    # Filepaths for rasters
    raw_quad_path = os.path.join(LIDAR_DIR, data_block, 'raw', quad)  # This holds the full path to the LIDAR dataset
    processed_quad_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_belowHMT.img".format(quad))  # This holds the full path to the output LIDAR dataset
    assert(os.path.exists(raw_quad_path)), "The path for quad {0} does not exist!:\r\n  {1}".format(quad, quad_path)  # Test to make sure quad_path exists
    lidar_paths_full.append(raw_quad_path)
    
    # Get the filesize
    quad_filesize = gm_fs.get_size(raw_quad_path)
    logger.info("  Filesize: {1} MB".format(quad, quad_filesize['MB']))
    
    ##
    # TIDAL conversion grid work
    ##
    logger.info("  ################### Reprojecting / resampling tidal conversion quads to match LIDAR tiles ###################")
    # Paths
    tss_path = os.path.join(TIDALDATUMS_DIR, "tss_merged_epsg2992_filled_invdist.img")  # TSS source grid
    mhhw_path = os.path.join(TIDALDATUMS_DIR, "mhhw_merged_epsg2992_filled_invdist.img")  # MHHW source grid
    mllw_path = os.path.join(TIDALDATUMS_DIR, "mllw_merged_epsg2992_filled_invdist.img")  # MLLW source grid
    processed_tss_quad_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_tss_conversion.img".format(quad))  # Output file
    processed_mhhw_quad_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_mhhw_conversion.img".format(quad))  # Output file
    processed_mllw_quad_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_mllw_conversion.img".format(quad))  # Output file
    # Work
    logger.info("  Reshaping TSS Quad")
    processed_tss_quad_path = hmt.reproject_dataset_to_quad(tss_path, raw_quad_path, processed_tss_quad_path, band=1, respample_method=gdal.GRA_Bilinear, maxmem=500, output_driver="HFA")  # Do the work
    #tss_tile = processed_tss_quad_path
    logger.info("    done.")
    logger.info("  Reshaping MHHW Quad")
    processed_mhhw_quad_path = hmt.reproject_dataset_to_quad(mhhw_path, raw_quad_path, processed_mhhw_quad_path, band=1, respample_method=gdal.GRA_Bilinear, maxmem=500, output_driver="HFA")  # Do the work
    #mhhw_tile = processed_mhhw_quad_path
    logger.info("    done.")
    #logger.info("  Reshaping MLLW Quad")
    #mllw_tile = hmt.reproject_dataset_to_quad(mllw_path, raw_quad_path, processed_mllw_quad_path, band=1, respample_method=gdal.GRA_Bilinear, maxmem=500, output_driver="HFA")  # Do the work
    #logger.info("    done.")
    logger.info("  ####### done.")
    
    ##
    # Convert LIDAR data to MHHW datum
    ##
    logger.info("  ################### Converting NAVD88 to MHHW datum using mhhw_tile ###################")
    lidar_in_mhhw_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_lidar_in_mhhw.img".format(quad))  # Output file
    lidar_in_mhhw_path = hmt.convert_navd88_to_tidal(raw_quad_path, processed_tss_quad_path, processed_mhhw_quad_path, lidar_in_mhhw_path)
    logger.info(" done.")
    logger.info("  ####### done.")
    
    logger.info("  ################### Reshaping HMT (in MHHW datum) to match LIDAR quad ###################")
    hmt_incriment_mhhw_path = os.path.join(TIDALINCRIMENT_DIR, 'dlcd_hmt_mhhw_nearest.img')
    hmt_incriment_mhhw_path_quad = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_hmt_incriment_mhhw.img".format(quad))  # Output file
    hmt_incriment_mhhw_path_quad = hmt.reproject_dataset_to_quad(hmt_incriment_mhhw_path, raw_quad_path, hmt_incriment_mhhw_path_quad, band=1, respample_method=gdal.GRA_Bilinear, maxmem=500, output_driver="HFA")  # Do the work
    logger.info("  ####### done.")
    
    ##
    # Convert LIDAR data to MLLW datum
    ##
    #logger.info("  ################### Converting NAVD88 to MLLW datum using mllw_tile ###################")
    #lidar_in_mllw_path = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_lidar_in_mllw.img".format(quad))  # Output file
    #lidar_in_mllw_path = hmt.convert_navd88_to_tidal(raw_quad_path, tss_tile, mllw_tile, lidar_in_mllw_path)
    #logger.info(" done.")
    #logger.info("  ####### done.")
    
    ##
    # Process raster to binary below HMT / above HMT raster via MHHW incriment
    ##
    logger.info("  ################### Processing binary raster based on MHHW incriment ###################")
    binary_raster_path_mhhw = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_HMT_binary_via_MHHW.img".format(quad))                     # Output file
    binary_raster_path_mhhw = hmt.hmt_tile_binary_processor_griddedHMT(lidar_in_mhhw_path, hmt_incriment_mhhw_path, binary_raster_path_mhhw)   # Create binary raster
    output_vector_path_mhhw = os.path.join(LIDAR_DIR, data_block, 'shp', "{0}_belowHMT_viaMHHW.shp".format(quad))                              # Vector filepath
    if os.path.exists(output_vector_path_mhhw): ogr.GetDriverByName("ESRI Shapefile").DeleteDataSource(output_vector_path_mhhw)                # Delete if exists
    output_vector_path_mhhw = hmt.binary_raster_to_vector(binary_raster_path_mhhw, output_vector_path_mhhw, driver="ESRI Shapefile")           # Create shapefile from binary raster
    logger.info("  Deleting binary raster...")
    gdal.GetDriverByName("HFA").Delete(binary_raster_path_mhhw)  # Delete the binary raster
    logger.info("  done.")
    logger.info("  ####### done.")
    
    ##
    # Process raster to binary below HMT / above HMT raster via MLLW incriment
    ##
    #logger.info("  ################### Processing binary raster based on MLLW incriment ###################")
    #binary_raster_path_mllw = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_HMT_binary_via_MLLW.img".format(quad))            # Output file
    #binary_raster_path_mllw = hmt.hmt_tile_binary_processor(lidar_in_mllw_path, 11.62, binary_raster_path_mllw)                         # Create binary raster
    #output_vector_path_mllw = os.path.join(LIDAR_DIR, data_block, 'shp', "{0}_belowHMT_viaMLLW.shp".format(quad))                     # Vector filepath
    #if os.path.exists(output_vector_path_mllw): ogr.GetDriverByName("ESRI Shapefile").DeleteDataSource(output_vector_path_mllw)       # Delete if exists
    #output_vector_path_mllw = hmt.binary_raster_to_vector(binary_raster_path_mhhw, output_vector_path_mllw, driver="ESRI Shapefile")  # Create shapefile from binary raster
    #logger.info("  Deleting binary raster...")
    #gdal.GetDriverByName("HFA").Delete(binary_raster_path_mllw)  # Delete the binary raster
    #logger.info("  done.")
    #logger.info("  ####### done.")
    
    
    ##
    # Process raster to binary below HMT / above HMT raster via NAVD88 incriment
    ##
    #logger.info("  ################### Processing binary raster based on NAVD88 ###################")
    #binary_raster_path_navd = os.path.join(LIDAR_DIR, data_block, 'processed', "{0}_HMT_binary_via_NAVD88.img".format(quad))          # Output file
    #binary_raster_path_navd = hmt.hmt_tile_binary_processor(raw_quad_path, 11.23, binary_raster_path_navd)                              # Create binary raster
    #output_vector_path_navd = os.path.join(LIDAR_DIR, data_block, 'shp', "{0}_belowHMT_viaNAVD88.shp".format(quad))                   # Vector filepath
    #if os.path.exists(output_vector_path_navd): ogr.GetDriverByName("ESRI Shapefile").DeleteDataSource(output_vector_path_navd)       # Delete if exists
    #output_vector_path_navd = hmt.binary_raster_to_vector(binary_raster_path_navd, output_vector_path_navd, driver="ESRI Shapefile")  # Create shapefile from binary raster
    #logger.info("  Deleting binary raster...")
    #gdal.GetDriverByName("HFA").Delete(binary_raster_path_navd)  # Delete the binary raster
    #logger.info("  done.")
    #logger.info("  ####### done.")
    
    # Clean up and exit
    #output_quads.append(binary_raster_path)
    #output_shps.append(vector_path)
    
    logger.info(" done.")
  
  print
  print "Raster Quads: "
  print ' '.join(output_quads)  
  print 
  print "Shapefile Quads: "
  print ' '.join(output_shps)

if __name__ == '__main__':
  # Each quad takes about 30 minutes (2012-06-05) on the old MacBook Pro (2.6 GHz Intel Core 2 Duo, 4gb 667 MHz DDR2 RAM)
  data_processor("SSNERR", 'SSNERR_LIDAR', ['be42124d3', 'be43124b2', 'be43124c1', 'be43124c2', 'be43124c3', 'be43124d1', 'be43124d2'], small=False)
  data_processor("Nehalem", 'Neh_LIDAR', ['be45123f8', 'be45123f7', 'be45123g7b'], small=False)
  data_processor("Tillamook", 'Till_LIDAR', ['be45123e8', 'be45123e7', 'be45123d8', 'be45123d7', 'be45123d6'], small=False)
  #logging.warn("Enable one of the processors above.")
  pass