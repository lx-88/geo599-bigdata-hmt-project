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

# Import Parallel Python
import pp

# Import Numpy
import numpy as np

# Import GDAL et al.
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

# Fix osgeo error reporting
gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

# Set gdal configuration parameters
gdal.SetCacheMax(2147483648)  # 2 GB. This sets the caching max size for gdal. Bigger number can lead to faster reads / writes
gdal.SetConfigOption('HFA_USE_RRD', 'YES')  # Configure GDAL to use blocks

import hmt_gdal

def reproject_dataset_to_quad(src_dataset_path, template_dataset_path, destination_dataset_path, band=1, respample_method=gdal.GRA_NearestNeighbour, maxmem=500, output_driver="HFA"):
  """
  Resample / Reproject a dataset to match the spatial extent and cell size of a template dataset.
  """
  logger.info("    Warping src dataset (using template) to dest dataset...")
  # Open the output driver
  output_drv = gdal.GetDriverByName( output_driver )
  
  # Setup the spatial refereneces
  src_dataset = gdal.Open(src_dataset_path, gdal.GA_ReadOnly)
  src_dataset_driver = src_dataset.GetDriver()
  src_srs = osr.SpatialReference()
  src_srs.ImportFromWkt(src_dataset.GetProjection())
  target_srs = osr.SpatialReference()
  target_srs = src_srs
    
  # Get source band metadata
  data_band = src_dataset.GetRasterBand(band)
  data_band_type = data_band.DataType
  #logger.info("    data_band_type: {0}".format(data_band_type))
  src_projection = src_dataset.GetProjection()  # Get Projection
  
  # Get the source geotransform vector
  src_geo_t = src_dataset.GetGeoTransform()  # top left x, w-e pixel res, rotation, top left y, rotation, n-s pixel res
  src_originX = src_geo_t[0]
  src_originY = src_geo_t[3]
  src_pixelWidth = src_geo_t[1]
  src_pixelHeight = src_geo_t[5]
  src_fullsize_num_cols = src_dataset.RasterXSize # Raster xsize
  src_fullsize_num_rows = src_dataset.RasterYSize # Raster ysize
    
  #logger.info('    src_pixelWidth: {0}'.format(src_pixelWidth))
  #logger.info('    src_pixelHeight: {0}'.format(src_pixelHeight))
  #logger.info("    src_fullsize_num_cols: {0}".format(src_fullsize_num_cols))
  #logger.info("    src_fullsize_num_rows: {0}".format(src_fullsize_num_rows))
  
  # Get template dataset
  # Open the binary tile as read-only and get the driver GDAL is using to access the data
  template_fh = gdal.Open(template_dataset_path, gdal.GA_ReadOnly)
  remplate_driver = template_fh.GetDriver()
  
  # Pull Metadata associated with the template tile so we can create the output raster later
  template_geotransform = template_fh.GetGeoTransform()
  tempalte_projection = template_fh.GetProjection()
  template_cols = template_fh.RasterXSize  # Get the number of columns
  template_rows = template_fh.RasterYSize  # Get the number of rows
  #template_nodata = template_fh.GetNoDataValue()  # Get the NoData value so we can set our mask
    
  # Hold the window data in a new memory-based raster so we can reproject
  logger.info("      creating new dataset...")
  outut_mhhw_dataset = output_drv.Create(destination_dataset_path, template_cols, template_rows, 1, data_band_type)
  outut_mhhw_dataset.SetGeoTransform(template_geotransform)
  outut_mhhw_dataset.SetProjection(tempalte_projection)
  logger.info("        done.")

  # Perform the projection/resampling
  logger.info("      reshaping data raster to match template raster...")
  res = gdal.ReprojectImage( src_dataset, outut_mhhw_dataset, src_srs.ExportToWkt(), target_srs.ExportToWkt(), respample_method, maxmem )
  logger.info("        done.")
    
  # Clean up
  logger.info("      cleaning up / closing datasets...")
  src_dataset = None
  template_fh = None
  outut_mhhw_dataset = None
  logger.info("        done.")
  logger.info("    done.")
  
  return destination_dataset_path

def convert_navd88_to_tidal(lidar_path, tss_path, tidal_conversion_path, lidar_in_tidal_datum_path, band=1, blocksize=(600,600), driver="HFA"):
  """
  Convert the lidar tile (NAVD88) to TSS to Tidal vertical datum
  """
  # Open the LIDAR tile as read-only and get the metadata
  lidar_tile_fh = gdal.Open(lidar_path, gdal.GA_ReadOnly)
  input_driver = lidar_tile_fh.GetDriver()
  lidar_geotransform = lidar_tile_fh.GetGeoTransform()
  lidar_projection = lidar_tile_fh.GetProjection()
  lidar_cols = lidar_tile_fh.RasterXSize  # Get the number of columns
  lidar_rows = lidar_tile_fh.RasterYSize  # Get the number of rows
  logger.info("    lidar_cols: {0}".format(lidar_cols))
  logger.info("    lidar_rows: {0}".format(lidar_rows))
  lidar_band = lidar_tile_fh.GetRasterBand(band)  # Get the raster band
  lidar_nodata = lidar_band.GetNoDataValue()  # Get the NoData value so we can set our mask
  lidar_datatype = lidar_band.DataType  # Doesn't work?!?
  lidar_tl_origin_x = lidar_geotransform[0]
  lidar_tl_origin_y = lidar_geotransform[3]
  lidar_pixelWidth = lidar_geotransform[1]
  lidar_pixelHeight = lidar_geotransform[5]
  
  # Open the NAVD88 - TSS conversion dataset as read-only and get metadata
  tss_conversion_fh = gdal.Open(tss_path, gdal.GA_ReadOnly)
  tss_conversion_driver = tss_conversion_fh.GetDriver()
  tss_conversion_geotransform = tss_conversion_fh.GetGeoTransform()
  tss_conversion_projection = tss_conversion_fh.GetProjection()
  tss_conversion_cols = tss_conversion_fh.RasterXSize  # Get the number of columns
  tss_conversion_rows = tss_conversion_fh.RasterYSize  # Get the number of rows
  logger.info("    tss_conversion_cols: {0}".format(tss_conversion_cols))
  logger.info("    tss_conversion_rows: {0}".format(tss_conversion_rows))
  tss_conversion_band = tss_conversion_fh.GetRasterBand(band)  # Get the raster band
  tss_conversion_nodata = tss_conversion_band.GetNoDataValue()  # Get the NoData value so we can set our mask
  tss_conversion_datatype = tss_conversion_band.DataType
  tss_conversion_tl_origin_x = tss_conversion_geotransform[0]
  tss_conversion_tl_origin_y = tss_conversion_geotransform[3]
  tss_conversion_pixelWidth = tss_conversion_geotransform[1]
  tss_conversion_pixelHeight = tss_conversion_geotransform[5]
  
  # Open the TSS to Tidal Datum conversion tile and get metadata
  tidal_conversion_fh = gdal.Open(tidal_conversion_path, gdal.GA_ReadOnly)
  tidal_conversion_driver = tidal_conversion_fh.GetDriver()
  tidal_conversion_geotransform = tidal_conversion_fh.GetGeoTransform()
  tidal_conversion_projection = tidal_conversion_fh.GetProjection()
  tidal_conversion_cols = tidal_conversion_fh.RasterXSize  # Get the number of columns
  tidal_conversion_rows = tidal_conversion_fh.RasterYSize  # Get the number of rows
  logger.info("    tidal_conversion_cols: {0}".format(tidal_conversion_cols))
  logger.info("    tidal_conversion_rows: {0}".format(tidal_conversion_rows))
  tidal_conversion_band = tidal_conversion_fh.GetRasterBand(band)  # Get the raster band
  tidal_conversion_nodata = tidal_conversion_band.GetNoDataValue()  # Get the NoData value so we can set our mask
  tidal_datatype = tidal_conversion_band.DataType
  tidal_conversion_tl_origin_x = tidal_conversion_geotransform[0]
  tidal_conversion_tl_origin_y = tidal_conversion_geotransform[3]
  tidal_conversion_pixelWidth = tidal_conversion_geotransform[1]
  tidal_conversion_pixelHeight = tidal_conversion_geotransform[5]
  
  # Do some tests to make sure that the grids line up
  assert(lidar_geotransform == tss_conversion_geotransform == tidal_conversion_geotransform), "geotransforms are not equivelent"
  if lidar_projection != tss_conversion_projection != tidal_conversion_projection: logger.warn("  Projections are not equal!")
  assert(lidar_cols == tss_conversion_cols == tidal_conversion_cols), "cols are not equivelent"
  assert(lidar_rows == tss_conversion_rows == tidal_conversion_rows), "cols are not equivelent"
  assert(lidar_tl_origin_x == tss_conversion_tl_origin_x == tidal_conversion_tl_origin_x), "origin x are not equivelent"
  assert(lidar_tl_origin_y == tss_conversion_tl_origin_y == tidal_conversion_tl_origin_y), "origin y are not equivelent"
  assert(lidar_pixelWidth == tss_conversion_pixelWidth == tidal_conversion_pixelWidth), "pixel width are not equivelent"
  assert(lidar_pixelHeight == tss_conversion_pixelHeight == tidal_conversion_pixelHeight), "pixel height are not equivelent"
  
  # Get block size from parameters
  xBlockSize = blocksize[0]
  yBlockSize = blocksize[1]
  
  cols = lidar_cols
  rows = lidar_rows
  
  num_block_cols = cols/xBlockSize  
  num_block_rows = rows/yBlockSize
  
  logger.info("    col blocks: {0}".format(num_block_cols))
  logger.info("    row blocks: {0}".format(num_block_rows))
  
  # Create a copy of the data using in the input tile as an example.
  logger.info("    Creating new raster...")
  output_driver = gdal.GetDriverByName(driver)  # Setup the output driver
  lidar_in_tidal_fh = output_driver.Create(lidar_in_tidal_datum_path, lidar_cols, lidar_rows, 1, lidar_datatype)
  lidar_in_tidal_fh.SetGeoTransform(lidar_geotransform)
  lidar_in_tidal_fh.SetProjection(lidar_projection)
  lidar_in_tidal_band = lidar_in_tidal_fh.GetRasterBand(1)
  lidar_in_tidal_band.SetNoDataValue(lidar_nodata)
  logger.info("      done.")
  
  logger.info("    Processing data...")
  
  #  Building Jobs
  job_n = 0
  for i in range(0, rows, yBlockSize):  # Loop through row blocks
    if i + yBlockSize < rows: numRows = yBlockSize
    else: numRows = rows - i
    for j in range(0, cols, xBlockSize):  # Loop through col blocks
      if j + xBlockSize < cols: numCols = xBlockSize
      else: numCols = cols - j
      # Build job here
      logger.info("      Working on block offset ({0},{1}); cols {2}; rows: {3}...".format(j,i, numCols, numRows))
      
      # Read data
      lidar_np = lidar_band.ReadAsArray(j, i, numCols, numRows)
      tss_conversion_np = tss_conversion_band.ReadAsArray(j, i, numCols, numRows)
      tidal_conversion_np = tidal_conversion_band.ReadAsArray(j, i, numCols, numRows)
      
      ##
      # Convert conversion grids to Survey Feet
      ##
      # 
      # NAVD88 unit = Intl Feet = 0.3048 m
      #   1 meter = 3.280833333 Survey Feet
      # 
      # TSS unit = meters
      # TSS_surveyft = TSS*3.280833333
      #
      # Tidal conversion unit = meters
      # tidal_conversion_surveyft = tss_conversion*3.280833333
      tss_conversion_np*3.280833333
      tidal_conversion_np*3.280833333
      
      ##
      # Convert NAVD88 to TSS to Tidal Datum
      ##
      # TSS = "Location of NAVD88 relative to LMSL"
      # Tidal Conversion = Location of Tidal Datum relative to LMSL
      # 
      # Therefore, ELEV_tidal = ELEV_navd + TSS conversion - Tidal conversion
      # 
      lidar_in_tidal = lidar_np+tss_conversion_np-tidal_conversion_np
      
      # Write the array to the raster
      lidar_in_tidal_band.WriteArray(lidar_in_tidal, j, i)
      
      # Clean Up
      lidar_np = None
      tss_conversion_np = None
      tidal_conversion_np = None
      lidar_in_tidal = None
      lidar_in_tidal_fh.FlushCache()
      
  # Done looping through blocks
  logger.info("   done.")
  
  # Compute Statistics before closing out the dataset
  logger.info("  Computing stats...")
  try:
    lidar_in_tidal_band.ComputeStatistics(False)
  except RuntimeError:
    logger.warn("    Cannot compute statistics. This probably means that there were no pixels that met the HMT definition and are therefore null values.")
  logger.info("    done.")
  
  logger.info("  Building blocks...")
  lidar_in_tidal_fh.BuildOverviews(overviewlist=[2,4,8,16,32,64,128])
  logger.info("    done.")
  
  logger.info("  Flushing the cache...")
  lidar_in_tidal_fh.FlushCache()
  logger.info("    done.")
  
  # Clean up the dataset file handlers
  logger.info("  Closing the dataset...")
  lidar_in_tidal_band = None
  lidar_in_tidal_fh = None
  mhhw_raster_resampled_fh = None
  logger.info("    done.")
  
  # We're done!
  #logger.info("  Done.")
  return lidar_in_tidal_datum_path
  

def convert_lidar_to_mhhw(tile_path, mhhw_path, hmt_value, output_path, driver="HFA", noData=0, blocksize=(600,600)):
  # Open the LIDAR tile as read-only and get the driver GDAL is using to access the data
  lidar_tile_fh = gdal.Open(tile_path, gdal.GA_ReadOnly)
  input_driver = lidar_tile_fh.GetDriver()
  
  # Open the MHHW dataset as read-only
  mhhw_fh = gdal.Open(mhhw_path, gdal.GA_ReadOnly)
  mhhw_driver = mhhw_fh.GetDriver()
  
  # Pull Metadata associated with the LIDAR tile so we can create the output raster later
  tile_geotransform = lidar_tile_fh.GetGeoTransform()
  tile_projection = lidar_tile_fh.GetProjection()
  cols = lidar_tile_fh.RasterXSize  # Get the number of columns
  rows = lidar_tile_fh.RasterYSize  # Get the number of rows
  logger.info("  cols: {0}".format(cols))
  logger.info("  rows: {0}".format(rows))
  tile_lidar = lidar_tile_fh.GetRasterBand(1)  # Get the raster band
  tile_nodata = tile_lidar.GetNoDataValue()  # Get the NoData value so we can set our mask
  tile_tl_origin_x = tile_geotransform[0]
  tile_tl_origin_y = tile_geotransform[3]
  tile_pixelWidth = tile_geotransform[1]
  tile_pixelHeight = tile_geotransform[5]
  
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
  mhhw_raster_resampled_fh = output_driver.Create(output_path, cols, rows, 1, gdal.GDT_Float32)
  mhhw_raster_resampled_fh.SetGeoTransform(tile_geotransform)
  mhhw_raster_resampled_fh.SetProjection(tile_projection)
  mhhw_resampled_band = mhhw_raster_resampled_fh.GetRasterBand(1)
  mhhw_resampled_band.SetNoDataValue(noData)
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
      
      # Pixel Coodinates
      minx_px = j         # left
      miny_px = i+numRows # bottom
      maxx_px = j+numCols # right
      maxy_px = i         # top
      cellsize_x = tile_pixelWidth
      cellsize_y = tile_pixelHeight
      
      logger.info("      numRows: {0}".format(numRows))
      logger.info("      numCols: {0}".format(numCols))
      logger.info("      cellsize_x: {0}".format(cellsize_x))
      logger.info("      cellsize_y: {0}".format(cellsize_y))
      logger.info("      maxx_px-minx_px: {0}".format(maxx_px-minx_px))
      
      # World Coodinates
      topleft_x = tile_tl_origin_x+((minx_px+1)*cellsize_x)
      topleft_y = tile_tl_origin_y+((maxy_px+1)*cellsize_y)
      bottomright_x = tile_tl_origin_x+((maxx_px+1)*cellsize_x)  # same as top right
      bottomright_y = tile_tl_origin_y+((miny_px+1)*cellsize_y)
      
      logger.info("      topleft: ({0}, {1})".format(topleft_x, topleft_y))
      logger.info("      bottomright: ({0}, {1})".format(bottomright_x, bottomright_y))
      
      lidar_window = {
        'cellsize_x': tile_pixelWidth,
        'cellsize_y': tile_pixelHeight,
        'topleft_x': topleft_x,
        'bottomright_x': bottomright_x,
        'topleft_y': bottomleft_y,
        'topleft_y': topleft_y,
        'cols': numCols,
        'rows': numRows,
      }
      
      #logger.info("window_dict: {0}".format(lidar_window))
      
      logger.info("      reproject mhhw to block window...")
      mhhw_window_ds = hmt_gdal.reproject_window_offset(mhhw_fh, lidar_window['topleft_x'], lidar_window['topleft_y'], lidar_window['cols'], lidar_window['rows'], cellsize_x=lidar_window['cellsize_x'], cellsize_y=lidar_window['cellsize_y'], band=1, epsg_from=2992, epsg_to=2992)
      logger.info("        read mhhw as array...")
      mhhw_window_np = mhhw_window_ds.GetRasterBand(1).ReadAsArray()
      logger.info("          mhhw_window_np shape: {0}".format(mhhw_window_np.shape))
      logger.info("          mhhw_window_np min: {0}".format(mhhw_window_np.min()))
      logger.info("          mhhw_window_np max: {0}".format(mhhw_window_np.max()))
      logger.info("         done")
      #sys.exit(1)
      #lidar_hmt_masked_below_hmt = np.ma.masked_equal(lidar_np, tile_nodata, copy=False).filled(np.nan) <= hmt_value  # Create the mask
      
      # Write the array to the raster
      mhhw_resampled_band.WriteArray(mhhw_window_np, j, i)
      
      # Clean Up
      mhhw_window_np = None
      mhhw_raster_resampled_fh.FlushCache()
      lidar_hmt_masked_below_hmt = None
      lidar_np = None
  # Done looping through blocks
  
  logger.info("   done.")
  
  # Compute Statistics before closing out the dataset
  logger.info("  Computing stats...")
  try:
    mhhw_resampled_band.ComputeStatistics(False)
  except RuntimeError:
    logger.warn("    Cannot compute statistics. This probably means that there were no pixels that met the HMT definition and are therefore null values.")
  logger.info("    done.")
  
  logger.info("  Building blocks...")
  mhhw_raster_resampled_fh.BuildOverviews(overviewlist=[2,4,8,16,32,64,128])
  logger.info("    done.")
  
  logger.info("  Flushing the cache...")
  lidar_tile_fh.FlushCache()
  logger.info("    done.")
  
  # Clean up the dataset file handlers
  logger.info("  Closing the dataset...")
  mhhw_resampled_band = None
  lidar_tile_fh = None
  mhhw_raster_resampled_fh = None
  logger.info("    done.")
  
  # We're done!
  #logger.info("  Done.")
  return output_path

def binary_raster_to_vector(binary_raster_path, output_vector_path, driver="ESRI Shapefile"):
  """ Convert a binary raster to a vector """
  
  # Open the binary tile as read-only and get the driver GDAL is using to access the data
  binary_tile_fh = gdal.Open(binary_raster_path, gdal.GA_ReadOnly)
  input_raster_driver = binary_tile_fh.GetDriver()
  
  # Pull Metadata associated with the binary tile so we can create the output raster later
  raster_geotransform = binary_tile_fh.GetGeoTransform()
  raster_projection = binary_tile_fh.GetProjection()
  cols = binary_tile_fh.RasterXSize  # Get the number of columns
  rows = binary_tile_fh.RasterYSize  # Get the number of rows
  binary_tile = binary_tile_fh.GetRasterBand(1)  # Get the raster band
  binary_nodata = binary_tile.GetNoDataValue()  # Get the NoData value so we can set our mask
  
  # Create the spatial ref for the output vector
  osr_ref = osr.SpatialReference()
  osr_ref.ImportFromWkt(raster_projection)
  
  # Create a memory OGR datasource to put results in.
  vect_driver = ogr.GetDriverByName(driver)
  vect_datasrc = vect_driver.CreateDataSource( output_vector_path )

  # Create the layer
  vect_layer = vect_datasrc.CreateLayer( 'poly', osr_ref, ogr.wkbPolygon )

  # Create a field
  fd = ogr.FieldDefn( 'value', ogr.OFTInteger )
  vect_layer.CreateField( fd )
  
  # run the algorithm.
  logger.info("  Runing Polygonize()...")
  logger.info("    converting raster to vector...")
  result = gdal.Polygonize( binary_tile, binary_tile.GetMaskBand(), vect_layer, 0 )
  logger.info("      done.")
  
  # Clean up
  logger.info("    cleaning up and closing out of datasources...")
  vect_layer = None
  vect_datasrc = None
  binary_tile = None
  binary_tile_fh = None
  logger.info("      done.")
  return output_vector_path

def hmt_tile_binary_processor(tile_path, hmt_value, output_path, driver="HFA", noData=0, blocksize=(600,600)):
  """ Open the tile, get info about it, create a binary raster marking areas below HMT as a value of 1"""

  # Open the LIDAR tile as read-only and get the driver GDAL is using to access the data
  lidar_tile_fh = gdal.Open(tile_path, gdal.GA_ReadOnly)
  input_driver = lidar_tile_fh.GetDriver()

  # Pull Metadata associated with the LIDAR tile so we can create the output raster later
  tile_geotransform = lidar_tile_fh.GetGeoTransform()
  tile_projection = lidar_tile_fh.GetProjection()
  cols = lidar_tile_fh.RasterXSize  # Get the number of columns
  rows = lidar_tile_fh.RasterYSize  # Get the number of rows
  logger.info("  cols: {0}".format(cols))
  logger.info("  rows: {0}".format(rows))
  tile_lidar = lidar_tile_fh.GetRasterBand(1)  # Get the raster band
  tile_nodata = tile_lidar.GetNoDataValue()  # Get the NoData value so we can set our mask
  
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
  HMT_output_fh = output_driver.Create(output_path, cols, rows, 1, gdal.GDT_Float32)
  HMT_output_fh.SetGeoTransform(tile_geotransform)
  HMT_output_fh.SetProjection(tile_projection)
  HMT_output_band = HMT_output_fh.GetRasterBand(1)
  HMT_output_band.SetNoDataValue(noData)
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
      lidar_np = tile_lidar.ReadAsArray(j, i, numCols, numRows)
      lidar_hmt_masked_below_hmt = np.ma.masked_equal(lidar_np, tile_nodata, copy=False).filled(np.nan) <= hmt_value  # Create the mask
      
      # Write the array to the raster
      HMT_output_band.WriteArray(lidar_hmt_masked_below_hmt, j, i)
      
      # Clean Up
      HMT_output_fh.FlushCache()
      lidar_hmt_masked_below_hmt = None
      lidar_np = None
  # Done looping through blocks
  
  logger.info("   done.")
  
  # Compute Statistics before closing out the dataset
  logger.info("  Computing stats...")
  try:
    HMT_output_band.ComputeStatistics(False)
  except RuntimeError:
    logger.warn("    Cannot compute statistics. This probably means that there were no pixels that met the HMT definition and are therefore null values.")
  logger.info("    done.")
  
  logger.info("  Building blocks...")
  HMT_output_fh.BuildOverviews(overviewlist=[2,4,8,16,32,64,128])
  logger.info("    done.")
  
  logger.info("  Flushing the cache...")
  lidar_tile_fh.FlushCache()
  logger.info("    done.")
  
  # Clean up the dataset file handlers
  logger.info("  Closing the dataset...")
  HMT_output_band = None
  lidar_tile_fh = None
  HMT_output_fh = None
  logger.info("    done.")
  
  # We're done!
  #logger.info("  Done.")
  return output_path