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


gdal_fillnodata.py -md 0 mhhw_merged_v2.img -mask mhhw_merged_v2_mask.img mhhw_merged_v2_mask_filled_v2.img

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

# Import Numpy
import numpy as np

# Import GDAL et al.
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

# Import Geomatics Research helpers
from gmtools import filesystem as gm_fs
from gmtools import gdal as gm_gdal

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

# Path to the LIDAR datasets
VDATUM_GRIDS_DIR = os.path.join(PROJECT_DIR, 'data', 'tidal_datums')

def fix_nodata(input_path, output_path, desired_nodata=-9999, driver="HFA", blocksize=(600,600)):
  """
  This function takes the data from the grid dataset and restablishes the nodata field in GDAL.
  """
  
  # Filepaths
  logger.info('input: {0}'.format(input_path))
  logger.info('output: {0}'.format(output_path))
  
  # Open the VDatm grid as read-only and get the driver GDAL is using to access the data
  input_fh = gdal.Open(input_path, gdal.GA_ReadOnly)
  input_driver =input_fh.GetDriver()
  
  # Pull Metadata associated with the LIDAR tile so we can create the output raster later
  geotransform = input_fh.GetGeoTransform()
  projection = input_fh.GetProjection()
  cols = input_fh.RasterXSize  # Get the number of columns
  rows = input_fh.RasterYSize  # Get the number of rows
  logger.info("  cols: {0}".format(cols))
  logger.info("  rows: {0}".format(rows))
  grid_data = input_fh.GetRasterBand(1)  # Get the raster band
  grid_datatype = grid_data.DataType
  grid_original_nodata = grid_data.GetNoDataValue()  # Get the NoData value so we can set our mask
  logger.info("  original nodata value: {0}".format(grid_original_nodata))
  if grid_original_nodata is None:
    logger.warn("  Using a nodata value of -88.88 because the src file didn't define a nodata value.")
    grid_original_nodata = float(-88.8)
  logger.info("  desired nodata value: {0}".format(desired_nodata))
  
  # Get block size from parameters
  xBlockSize = blocksize[0]
  yBlockSize = blocksize[1]

  num_block_cols = cols/xBlockSize  
  num_block_rows = rows/yBlockSize
  
  logger.info("  col blocks: {0}".format(num_block_cols))
  logger.info("  row blocks: {0}".format(num_block_rows))
  
  # Create a copy of the data using in the input tile as an example.
  logger.info("  Creating new raster...")
  output_driver = gdal.GetDriverByName(driver)  # Setup the output driver
  output_fh = output_driver.Create(output_path, cols, rows, 1, grid_datatype)
  output_fh.SetGeoTransform(geotransform)
  output_fh.SetProjection(projection)
  output_band = output_fh.GetRasterBand(1)
  output_band.SetNoDataValue(desired_nodata)
  logger.info("    done.")
  
  logger.info("  Processing data...")
  
  #  Building Jobs
  job_n = 0
  for i in range(0, rows, yBlockSize):  # Loop through row blocks
    if i + yBlockSize < rows: numRows = yBlockSize
    else: numRows = rows - i
    for j in range(0, cols, xBlockSize):  # Loop through col blocks
      if j + xBlockSize < cols: numCols = xBlockSize
      else: numCols = cols - j
      # Build job here
      logger.info("    Working on block offset ({0},{1}); cols {2}; rows: {3}...".format(j,i, numCols, numRows))
      grid_np = grid_data.ReadAsArray(j, i, numCols, numRows)
      grid_np_masked = np.ma.masked_less_equal(grid_np, grid_original_nodata, copy=False).filled(np.NaN)  # Create the mask
      
      # Write the array to the raster
      output_band.WriteArray(grid_np_masked, j, i)
      
      # Clean Up
      output_fh.FlushCache()
      grid_np_masked = None
      grid_np = None
  # Done looping through blocks
  
  logger.info("   done.")
  
  # Compute Statistics before closing out the dataset
  logger.info("  Computing stats...")
  output_band.ComputeStatistics(False)
  logger.info("    done.")
  
  logger.info("  Building blocks...")
  output_fh.BuildOverviews(overviewlist=[2,4,8,16,32,64,128])
  logger.info("    done.")
  
  logger.info("  Flushing the cache...")
  output_fh.FlushCache()
  logger.info("    done.")
  
  # Clean up the dataset file handlers
  logger.info("  Closing the dataset...")
  output_band = None
  input_fh = None
  output_fh = None
  logger.info("    done.")
  
  return output_path

def create_nodata_mask(mhhw_path, output_path, desired_nodata=1, driver="HFA", blocksize=(600,600)):
  """
  This function takes the data from the merged dataset created using ArcGIS
  and restablishes the nodata field in GDAL.
  """
  
  logger.info('input: {0}'.format(mhhw_path))
  logger.info('output: {0}'.format(output_path))
  
  # Open the MHHW tile as read-only and get the driver GDAL is using to access the data
  mhhw_fh = gdal.Open(mhhw_path, gdal.GA_ReadOnly)
  input_driver = mhhw_fh.GetDriver()

  # Pull Metadata associated with the LIDAR tile so we can create the output raster later
  geotransform = mhhw_fh.GetGeoTransform()
  projection = mhhw_fh.GetProjection()
  cols = mhhw_fh.RasterXSize  # Get the number of columns
  rows = mhhw_fh.RasterYSize  # Get the number of rows
  logger.info("  cols: {0}".format(cols))
  logger.info("  rows: {0}".format(rows))
  mhhw_data = mhhw_fh.GetRasterBand(1)  # Get the raster band
  mhhw_original_nodata = mhhw_data.GetNoDataValue()  # Get the NoData value so we can set our mask
  
  logger.info("  original nodata value: {0}".format(mhhw_original_nodata))
  
  # Get block size from parameters
  xBlockSize = blocksize[0]
  yBlockSize = blocksize[1]

  num_block_cols = cols/xBlockSize  
  num_block_rows = rows/yBlockSize
  
  logger.info("  col blocks: {0}".format(num_block_cols))
  logger.info("  row blocks: {0}".format(num_block_rows))
  
  # Create a copy of the data using in the input tile as an example.
  logger.info("  Creating new raster...")
  output_driver = gdal.GetDriverByName(driver)  # Setup the output driver
  output_fh = output_driver.Create(output_path, cols, rows, 1, gdal.GDT_Byte)
  output_fh.SetGeoTransform(geotransform)
  output_fh.SetProjection(projection)
  output_band = output_fh.GetRasterBand(1)
  output_band.SetNoDataValue(desired_nodata)
  logger.info("    done.")
  
  logger.info("  Processing data...")
  
  #  Building Jobs
  job_n = 0
  for i in range(0, rows, yBlockSize):  # Loop through row blocks
    if i + yBlockSize < rows: numRows = yBlockSize
    else: numRows = rows - i
    for j in range(0, cols, xBlockSize):  # Loop through col blocks
      if j + xBlockSize < cols: numCols = xBlockSize
      else: numCols = cols - j
      # Build job here
      logger.info("    Working on block offset ({0},{1}); cols {2}; rows: {3}...".format(j,i, numCols, numRows))
      mhhw_np = mhhw_data.ReadAsArray(j, i, numCols, numRows)
      mhhw_np_masked = np.ma.masked_equal(mhhw_np, mhhw_original_nodata, copy=False).filled(0)  # Create the mask
      mhhw_np_masked = np.ma.masked_not_equal(mhhw_np_masked, 0, copy=False).filled(1)  # Create the mask
      
      # Write the array to the raster
      output_band.WriteArray(mhhw_np_masked, j, i)
      
      # Clean Up
      output_fh.FlushCache()
      mhhw_np_masked = None
      mhhw_np = None
  # Done looping through blocks
  
  logger.info("   done.")
  
  # Compute Statistics before closing out the dataset
  logger.info("  Computing stats...")
  output_band.ComputeStatistics(False)
  logger.info("    done.")
  
  logger.info("  Building blocks...")
  output_fh.BuildOverviews(overviewlist=[2,4,8,16,32,64,128])
  logger.info("    done.")
  
  logger.info("  Flushing the cache...")
  output_fh.FlushCache()
  logger.info("    done.")
  
  # Clean up the dataset file handlers
  logger.info("  Closing the dataset...")
  output_band = None
  mhhw_fh = None
  output_fh = None
  logger.info("    done.")
  
  return output_path

def fill_nodata(input_path, mask_path, output_path, max_distance=0, smoothing_iterations=0, options=[], driver="HFA", desired_nodata=-9999, quiet=False):
  """
  This function mimicks the gdal_fillnodata.py script because it's heavily based on it.
  Basically I just added more logging to fit it into this project.
  """
  
  logger.info('input: {0}'.format(input_path))
  logger.info('mask: {0}'.format(mask_path))
  logger.info('output: {0}'.format(output_path))
  
  # Open the MHHW tile as read-only and get the driver GDAL is using to access the data
  input_fh = gdal.Open(input_path, gdal.GA_ReadOnly)
  input_driver = input_fh.GetDriver()
  
  # Open the mask tile as read-only and get the driver GDAL is using to access the data
  mask_fh = gdal.Open(mask_path, gdal.GA_ReadOnly)
  mask_driver = mask_fh.GetDriver()
  mask_band = mask_fh.GetRasterBand(1)  # Get the raster band

  # Pull Metadata associated with the LIDAR tile so we can create the output raster later
  geotransform = input_fh.GetGeoTransform()
  projection = input_fh.GetProjection()
  cols = input_fh.RasterXSize  # Get the number of columns
  rows = input_fh.RasterYSize  # Get the number of rows
  logger.info("  cols: {0}".format(cols))
  logger.info("  rows: {0}".format(rows))
  input_data = input_fh.GetRasterBand(1)  # Get the raster band
  original_nodata = input_data.GetNoDataValue()  # Get the NoData value so we can set our mask
  logger.info("  original nodata value: {0}".format(original_nodata))
  
  # Create a copy of the data using in the input tile as an example.
  logger.info("  Creating new raster...")
  output_driver = gdal.GetDriverByName(driver)  # Setup the output driver
  output_fh = output_driver.Create(output_path, cols, rows, 1, gdal.GDT_CFloat32)
  output_fh.SetGeoTransform(geotransform)
  output_fh.SetProjection(projection)
  output_band = output_fh.GetRasterBand(1)
  output_band.SetNoDataValue(desired_nodata)
  logger.info("    done.")
  
  logger.info("  copying band to destination file...")
  gm_gdal.CopyBand( input_data, output_band )
  logger.info("    done.")
  
  # Suppress progress report if we ask for quiet behavior
  if quiet:
    prog_func = None
  else:
    prog_func = gdal.TermProgress
  
  logger.info("  Running FillNodata()...")
  result = gdal.FillNodata(output_band, mask_band, max_distance, smoothing_iterations, options, callback = prog_func)
  logger.info("    done.")
  
  return result

if __name__ == '__main__':
  """
  Run the steps to mess with the Tidal conversion grids provided by VDatum.
  They have already been reprojected and merged.
  
  This took 1.25 hours to run on the Linux box.
  """
  # Replace the nodata value supplied by ArcGIS with the desired nodata value.
  #for folder in ('CAORblan01_8301', 'OR_centr01_8301', 'ORWAcolr01_8301'):
  #  for grid_name in ('mhhw.img', 'mllw.img', 'tss.img'):
  #    inpath = os.path.join(VDATUM_GRIDS_DIR, 'shift_grids_hfa', folder, grid_name)
  #    outname = '__'.join([folder, grid_name])
  #    outpath = os.path.join(VDATUM_GRIDS_DIR, 'shift_grids_hfa', 'fixed_grids', outname)
  #    fix_nodata(inpath, outpath, desired_nodata=-9999)
  
  #mhhw_grids = list()
  #for folder in ('CAORblan01_8301', 'OR_centr01_8301', 'ORWAcolr01_8301'):
  #  for grid_name in ('mhhw.img',):
  #    outname = '__'.join([folder, grid_name])
  #    outpath = os.path.join(VDATUM_GRIDS_DIR, 'fixed_grids', outname)
  #    mhhw_grids.append(outpath)
  
  #print "MHHW grids:"
  #print ' '.join(mhhw_grids)
  
  #mllw_grids = list()
  #for folder in ('CAORblan01_8301', 'OR_centr01_8301', 'ORWAcolr01_8301'):
  #  for grid_name in ('mllw.img',):
  #    outname = '__'.join([folder, grid_name])
  #    outpath = os.path.join(VDATUM_GRIDS_DIR, 'fixed_grids', outname)
  #    mllw_grids.append(outpath)
  
  #print "MLLW grids:"
  #print ' '.join(mllw_grids)
  
  #tss_grids = list()
  #for folder in ('CAORblan01_8301', 'OR_centr01_8301', 'ORWAcolr01_8301'):
  #  for grid_name in ('tss.img',):
  #    outname = '__'.join([folder, grid_name])
  #    outpath = os.path.join(VDATUM_GRIDS_DIR, 'fixed_grids', outname)
  #    tss_grids.append(outpath)
  
  #print "TSS grids:"
  #print ' '.join(tss_grids)
  
  # Create the nodata mask of values. value=0 = nodata, value=1 = has data
  for grid_name in ('mhhw_merged_epsg2992.img', 'mllw_merged_epsg2992.img', 'tss_merged_epsg2992.img'):  
    filename_split = os.path.splitext(grid_name)  # split the extension from grid_name
    input_path = os.path.join(VDATUM_GRIDS_DIR, grid_name)  # path to the input grid
    output_path = os.path.join(VDATUM_GRIDS_DIR, filename_split[0]+"_mask"+filename_split[1])  # output path for grid
    create_nodata_mask(input_path, output_path)  # Do the work
  
  for grid_name in ('mhhw_merged_epsg2992.img', 'mllw_merged_epsg2992.img', 'tss_merged_epsg2992.img'):
    filename_split = os.path.splitext(grid_name)  # split the extension from grid_name
    grid_path = os.path.join(VDATUM_GRIDS_DIR, grid_name)  # path to the input grid
    mask_path = os.path.join(VDATUM_GRIDS_DIR, filename_split[0]+"_mask"+filename_split[1])
    output_path = os.path.join(VDATUM_GRIDS_DIR, filename_split[0]+"_filled_invdist"+filename_split[1])
    
    # Equivelent to gdal_fillnodata.py -md 0 mhhw_merged_v2.img -mask mhhw_merged_v2_mask.img mhhw_merged_v2_mask_filled_v2.img
    # max_distance= 0 means that the script is allowed to search the entire raster for values
    # takes about 45 min to run on Linux box.
    fillnodata_result = fill_nodata(grid_path, mask_path, output_path, max_distance=0)
    print fillnodata_result