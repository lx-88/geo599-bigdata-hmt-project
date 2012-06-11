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
import csv

# Import and configure logging
import logging
logger = logging.getLogger('hmt_processor')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
ch.setFormatter(formatter)
logger.addHandler(ch)
fhnd = logging.FileHandler('logs/tabulate_areas.log')
fhnd.setLevel(logging.DEBUG)
fhnd.setFormatter(formatter)
logger.addHandler(fhnd)


# Import Numpy
import numpy as np

# Import GDAL et al.
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

# Import Geomatics Research helpers
from gmtools import filesystem as gm_fs
from gmtools import geospatial as gm_gdal

# Import HMT specific packages
from hmt_processor import processors as hmt

# Fix osgeo error reporting
gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

# Set gdal configuration parameters
gdal.SetCacheMax(2147483648)  # 2 GB. This sets the caching max size for gdal. Bigger number can lead to faster reads / writes
gdal.SetConfigOption('HFA_USE_RRD', 'YES')  # Configure GDAL to use blocks

# get a reference to the path that holds this file
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# File folders that break up LIDAR tiles
SITE_BLOCKS = ['Neh_LIDAR', 'SSNERR_LIDAR', 'Till_LIDAR']


def dissolve_polygons():
  pass

def data_area_tabulator(name, output_csv_path, data_block, quads, simplify_tollerance=5, small=False):
  """
  
  """
  logger.info("Welcome to the {0} area tabulator!".format(name))
  logger.warn("  simplify tollerance is set to {0}".format(simplify_tollerance))
  
  # CSV setup
  output_csv = csv.writer(open(os.path.join(PROJECT_DIR, 'output', output_csv_path), 'w'))  # Setup the CSV writer
  output_csv.writerow(['block_name', 'quad', 'datum', 'area_under_HMT_sqft'])  # Write Column Headings
  
  shp_driver = ogr.GetDriverByName("ESRI Shapefile")
  
  output_filepath_mhhw = os.path.join(PROJECT_DIR, 'output', "{0}_merged_areas_viaMHHW.shp".format(name))
  if os.path.exists(output_filepath_mhhw): shp_driver.DeleteDataSource(output_filepath_mhhw)
  ds_mhhw = shp_driver.CreateDataSource( output_filepath_mhhw )
  
  output_filepath_navd88 = os.path.join(PROJECT_DIR, 'output', "{0}_merged_areas_viaNAVD88.shp".format(name))
  if os.path.exists(output_filepath_navd88): shp_driver.DeleteDataSource(output_filepath_navd88)
  ds_navd88 = shp_driver.CreateDataSource( output_filepath_navd88 )
  
  spatialReference = osr.SpatialReference()
  spatialReference.ImportFromEPSG(2992)
  
  layer_mhhw = ds_mhhw.CreateLayer(os.path.splitext(output_filepath_mhhw)[0], spatialReference, ogr.wkbMultiPolygon)
  layer_navd88 = ds_navd88.CreateLayer(os.path.splitext(output_filepath_navd88)[0], spatialReference, ogr.wkbMultiPolygon)
  
  field_defn = ogr.FieldDefn( "belowHMT", ogr.OFTString )
  field_defn.SetWidth( 5 )
  layer_mhhw.CreateField ( field_defn )
  layer_navd88.CreateField ( field_defn )
  
  geom_to_merge_mhhw = ogr.Geometry(type=ogr.wkbGeometryCollection)
  geom_to_merge_navd88 = ogr.Geometry(type=ogr.wkbGeometryCollection)
  
  # Loop through each quad
  for quad in quads:
    # Folder to shapefiles
    quad_shp_folder_path = os.path.join(PROJECT_DIR, 'data', 'LIDAR', data_block, 'shp')
    
    # HMT via vertical datums
    quad_shp_path_viaMHHW = os.path.join(quad_shp_folder_path, "{0}_belowHMT_viaMHHW.shp".format(quad))
    quad_shp_path_viaNAVD88 = os.path.join(quad_shp_folder_path, "{0}_belowHMT_viaNAVD88.shp".format(quad))
    
    # Make sure that the shapefile exists. If it doesn't throw a warning and move to the next quad
    if os.path.exists(quad_shp_path_viaMHHW) is False:
      logger.error("The HMT shapefile (via MHHW) for quad {0} doesn't exist! Skipping!".format(quad))
      continue
    
    # Make sure that the shapefile exists. If it doesn't throw a warning and move to the next quad
    if os.path.exists(quad_shp_path_viaNAVD88) is False:
      logger.error("The HMT shapefile (via NAVD88) for quad {0} doesn't exist! Skipping!".format(quad))
      continue
    
    # Open the shapefile using OGR
    quad_vect_fp_mhhw = ogr.Open(quad_shp_path_viaMHHW)
    if quad_vect_fp_mhhw is None:
      logger.error("Could not open shapefile: {0}".format(quad_shp_path_viaMHHW))
      continue
    
    # Open the shapefile using OGR
    quad_vect_fp_navd88 = ogr.Open(quad_shp_path_viaNAVD88)
    if quad_vect_fp_navd88 is None:
      logger.error("Could not open shapefile: {0}".format(quad_shp_path_viaNAVD88))
      continue
    
    quad_vect_driver_mhhw = quad_vect_fp_mhhw.GetDriver()
    quad_vect_layer_mhhw = quad_vect_fp_mhhw.GetLayer()
    feat_defn_mhhw = quad_vect_layer_mhhw.GetLayerDefn()
    
    quad_vect_driver_navd88 = quad_vect_fp_navd88.GetDriver()
    quad_vect_layer_navd88 = quad_vect_fp_navd88.GetLayer()
    feat_defn_navd88 = quad_vect_layer_navd88.GetLayerDefn()
    
    logger.info("  Looping through MHHW features...")
    # Loop through each feature in the layer in the MHHW layer
    for feature in quad_vect_layer_mhhw:
      geom = feature.GetGeometryRef().Simplify(simplify_tollerance)
      geom_to_merge_mhhw.AddGeometry(geom)
    logger.info("    done.")
    
    logger.info("  Looping through NAVD88 features...")
    # Loop through each feature in the layer in the NAVD88 layer
    for feature in quad_vect_layer_navd88:
      geom = feature.GetGeometryRef().Simplify(simplify_tollerance)
      geom_to_merge_navd88.AddGeometry(geom)
    logger.info("    done.")
      
  # This dissolves the overlapping regions of polygon components
  logger.info("  Dissolving MHHW features...")
  gb_mhhw = geom_to_merge_mhhw.Buffer(0)
  logger.info("    done.")
  
  # This dissolves the overlapping regions of polygon components
  logger.info("  Dissolving NAVD88 features...")
  gb_navd88 = geom_to_merge_navd88.Buffer(0)
  logger.info("    done.")
  
  logger.info("  Creating NA feature...")
  layerDefinition_mhhw = layer_mhhw.GetLayerDefn()
  layerDefinition_navd88 = layer_navd88.GetLayerDefn()
  
  feature_mhhw = ogr.Feature(layerDefinition_mhhw)
  feature_mhhw.SetField( "belowHMT", "Yes" )
  feature_mhhw.SetGeometry(gb_mhhw)
  layer_mhhw.CreateFeature(feature_mhhw)
  feature_mhhw.Destroy()
  
  feature_navd88 = ogr.Feature(layerDefinition_navd88)
  feature_navd88.SetField( "belowHMT", "Yes" )
  feature_navd88.SetGeometry(gb_navd88)
  layer_navd88.CreateFeature(feature_navd88)
  feature_navd88.Destroy()
  
  logger.info("  done.")
  
  # Get area of dissolved shapefile
  new_area_mhhw = gb_mhhw.GetArea()
  
  # Get area of dissolved shapefile
  new_area_navd88 = gb_navd88.GetArea()
  
  # Write output to CSV
  output_csv.writerow([name, 'MHHW', new_area_mhhw])  # Write Column Headings
  output_csv.writerow([name, 'NAVD88', new_area_navd88])  # Write Column Headings
  
  return output_csv_path

if __name__ == '__main__':
  """
  blah
  """
  
  # Each quad takes about 30 minutes (2012-06-05) on the old MacBook Pro (2.6 GHz Intel Core 2 Duo, 4gb 667 MHz DDR2 RAM)
  data_area_tabulator("SSNERR", "SSNERR_areas.csv", 'SSNERR_LIDAR', ['be42124d3', 'be43124b2', 'be43124c1', 'be43124c2', 'be43124c3', 'be43124d1', 'be43124d2', '43124d3','43124e3','43124e2'], small=False)
  data_area_tabulator("Nehalem", "Nehalem_areas.csv", 'Neh_LIDAR', ['be45123f8', 'be45123f7', 'be45123g7b'], small=False)
  data_area_tabulator("Tillamook", "Tillamook_areas.csv", 'Till_LIDAR', ['be45123e8', 'be45123e7', 'be45123d8', 'be45123d7', 'be45123d6'], small=False)
  #logging.warn("Enable one of the processors above.")
  pass