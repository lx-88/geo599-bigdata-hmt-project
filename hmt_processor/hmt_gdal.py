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

import gmtools.geospatial as gm_geo



def reproject_window_offset(src_dataset, topleft_x, topleft_y, window_xrange, window_yrange, desired_cellsize_x, desired_cellsize_y, band=1, epsg_from=None, epsg_to=None, respample_method=gdal.GRA_NearestNeighbour, blocksize=500, maxmem=500 ):
    """
    adapted from http://jgomezdans.github.com/gdal_notes/reprojection.html
    """
    logger.info("        setting up the metadata and window parameters...")
    logger.info("          topleft_x={0}, topleft_y={1}, window_xrange={2}, window_yrange={3}, desired_cellsize_x={4}, desired_cellsize_y={5}".format(topleft_x, topleft_y, window_xrange, window_yrange, desired_cellsize_x, desired_cellsize_y))
    
    # Open the memory driver
    mem_drv = gdal.GetDriverByName( 'MEM' )
    
    # Setup the spatial refereneces
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(src_dataset.GetProjection())
    
    target_srs = osr.SpatialReference()
    target_srs = src_srs
    
    # Get band metadata
    data_band = src_dataset.GetRasterBand(band)
    data_band_type = data_band.DataType
    logger.info("        data_band_type: {0}".format(data_band_type))
    
    # Get Projection
    src_projection = src_dataset.GetProjection()
    
    # Get the Geotransform vector
    src_geo_t = src_dataset.GetGeoTransform()  # top left x, w-e pixel res, rotation, top left y, rotation, n-s pixel res
    src_originX = src_geo_t[0]
    src_originY = src_geo_t[3]
    src_pixelWidth = src_geo_t[1]
    src_pixelHeight = src_geo_t[5]
    src_fullsize_num_cols = src_dataset.RasterXSize # Raster xsize
    src_fullsize_num_rows = src_dataset.RasterYSize # Raster ysize
    
    logger.info('        src_pixelWidth: {0}'.format(src_pixelWidth))
    logger.info('        src_pixelHeight: {0}'.format(src_pixelHeight))
    logger.info("        src_fullsize_num_cols: {0}".format(src_fullsize_num_cols))
    logger.info("        src_fullsize_num_rows: {0}".format(src_fullsize_num_rows))
    
    # Do coordinate translations between world and pixel coordinates
    window_xOffset_px, window_yOffset_px = gm_geo.World2PixelCoords(src_geo_t, topleft_x, topleft_y)   # Source Pixel Coodinates
    logger.info("        window_xOffset_px: {0}".format(window_xOffset_px))
    logger.info("        window_yOffset_px: {0}".format(window_yOffset_px))
    
    # Query the window
    logger.info("        extracting window from data...")
    window_array = data_band.ReadAsArray(window_left_x_px, window_top_y_px, window_xrange, window_yrange)
    logger.info("          window_array_shape: {0}".format(window_array.shape))
    logger.info("          window_array min: {0}".format(window_array.min()))
    logger.info("          window_array max: {0}".format(window_array.max()))
    logger.info("         done.")
    
    # Hold the window data in a new memory-based raster so we can reproject
    src_window_dataset = mem_drv.Create('', window_xrange, window_yrange, 1, data_band_type)
    new_geotransform =  [minx, pixelWidth, 0, maxy, 0, pixelHeight]
    #logger.info('window geotransform')
    #logger.info(new_geotransform)
    src_window_dataset.SetGeoTransform(new_geotransform)
    src_window_dataset.SetProjection(src_projection)
    window_src_band = src_window_dataset.GetRasterBand(1)
    window_src_band.WriteArray(window_array, 0, 0)
    window_src_band.FlushCache()
    window_src_band.ComputeStatistics(True)
    
    # Clean up
    window_array = None
    window_src_band = None
    data_band = None
    src_dataset = None
    
    # Calculate the new geotransform
    new_geo = ( minx, cellsize_x, 0, maxy, 0, cellsize_y )
    
    # Resampled Pixel Coodinates
    minx_px_rs, maxy_px_rs = gm_geo.World2PixelCoords(new_geo, minx, maxy)  # Top left
    maxx_px_rs, miny_px_rs = gm_geo.World2PixelCoords(new_geo, maxx, miny)  # Bottom right
    window_xrange_rs = maxx_px_rs - minx_px_rs
    window_yrange_rs = (maxy_px_rs - miny_px_rs)*-1  # *-1 because we have a negative cell size
    
    logger.info("        window_xrange_rs: {0}".format(window_xrange_rs))
    logger.info("        window_yrange_rs: {0}".format(window_yrange_rs))
    
    target_dataset = mem_drv.Create('', window_xrange_rs, window_yrange_rs, 1, data_band_type)
    
    # Set the geotransform
    #logger.info('resample geotransform')
    #logger.info(new_geo)
    target_dataset.SetGeoTransform( new_geo )
    target_dataset.SetProjection ( target_srs.ExportToWkt() )
    
    # Perform the projection/resampling
    logger.info("        reshaping MHHW raster to match window and cellsize...")
    res = gdal.ReprojectImage( src_window_dataset, target_dataset, src_srs.ExportToWkt(), target_srs.ExportToWkt(), respample_method, maxmem )
    logger.info("          done.")
    
    return target_dataset


def reproject_window(src_dataset, minx, maxx, miny, maxy, cellsize_x=None, cellsize_y=None, band=1, epsg_from=None, epsg_to=None, respample_method=gdal.GRA_NearestNeighbour, blocksize=500, maxmem=500 ):
    """
    adapted from http://jgomezdans.github.com/gdal_notes/reprojection.html
    """
    logger.info("        setting up the metadata and window parameters...")
    logger.info("          minx={0}, maxx={1}, miny={2}, maxy={3}".format(minx, maxx, miny, maxy))
    
    # Open the memory driver
    mem_drv = gdal.GetDriverByName( 'MEM' )
    
    # Setup the spatial refereneces
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(src_dataset.GetProjection())
    
    target_srs = osr.SpatialReference()
    target_srs = src_srs
    
    # Get band metadata
    data_band = src_dataset.GetRasterBand(band)
    data_band_type = data_band.DataType
    logger.info("        data_band_type: {0}".format(data_band_type))
    
    # Get Projection
    src_projection = src_dataset.GetProjection()
    
    # Get the Geotransform vector
    geo_t = src_dataset.GetGeoTransform()  # top left x, w-e pixel res, rotation, top left y, rotation, n-s pixel res
    originX = geo_t[0]
    originY = geo_t[3]
    pixelWidth = geo_t[1]
    pixelHeight = geo_t[5]
    fullsize_num_cols = src_dataset.RasterXSize # Raster xsize
    fullsize_num_rows = src_dataset.RasterYSize # Raster ysize
    
    logger.info('        pixelWidth: {0}'.format(pixelWidth))
    logger.info('        pixelHeight: {0}'.format(pixelHeight))
    logger.info("        fullsize_num_cols: {0}".format(fullsize_num_cols))
    logger.info("        fullsize_num_rows: {0}".format(fullsize_num_rows))
    
    # Do coordinate translations between world and pixel coordinates
    window_xOffset_px, window_yOffset_px = gm_geo.World2PixelCoords(geo_t, minx, maxy)   # Source Pixel Coodinates
    window_left_x_px, window_top_y_px = gm_geo.World2PixelCoords(geo_t, minx, maxy)      # Top left
    window_right_x_px, window_bottom_y_px = gm_geo.World2PixelCoords(geo_t, maxx, miny)  # Bottom right
    window_xrange = window_right_x_px - window_left_x_px                                 # window width in pixel units
    window_yrange = (window_top_y_px - window_bottom_y_px)*-1                            # *-1 because we have a negative cell size
    
    logger.info("        window_xOffset_px: {0}".format(window_xOffset_px))
    logger.info("        window_yOffset_px: {0}".format(window_yOffset_px))
    logger.info("        window_left_x_px: {0}".format(window_left_x_px))
    logger.info("        window_right_x_px: {0}".format(window_right_x_px))
    logger.info("        window_bottom_y_px: {0}".format(window_bottom_y_px))
    logger.info("        window_top_y_px: {0}".format(window_top_y_px))
    logger.info("        window_xrange: {0}".format(window_xrange))
    logger.info("        window_yrange: {0}".format(window_yrange))
    
    # Query the window
    logger.info("        extracting window from data...")
    window_array = data_band.ReadAsArray(window_left_x_px, window_top_y_px, window_xrange, window_yrange)
    logger.info("          window_array_shape: {0}".format(window_array.shape))
    logger.info("          window_array min: {0}".format(window_array.min()))
    logger.info("          window_array max: {0}".format(window_array.max()))
    logger.info("         done.")
    
    # Hold the window data in a new memory-based raster so we can reproject
    src_window_dataset = mem_drv.Create('', window_xrange, window_yrange, 1, data_band_type)
    new_geotransform =  [minx, pixelWidth, 0, maxy, 0, pixelHeight]
    #logger.info('window geotransform')
    #logger.info(new_geotransform)
    src_window_dataset.SetGeoTransform(new_geotransform)
    src_window_dataset.SetProjection(src_projection)
    window_src_band = src_window_dataset.GetRasterBand(1)
    window_src_band.WriteArray(window_array, 0, 0)
    window_src_band.FlushCache()
    window_src_band.ComputeStatistics(True)
    
    # Clean up
    window_array = None
    window_src_band = None
    data_band = None
    src_dataset = None
    
    # Calculate the new geotransform
    new_geo = ( minx, cellsize_x, 0, maxy, 0, cellsize_y )
    
    # Resampled Pixel Coodinates
    minx_px_rs, maxy_px_rs = gm_geo.World2PixelCoords(new_geo, minx, maxy)  # Top left
    maxx_px_rs, miny_px_rs = gm_geo.World2PixelCoords(new_geo, maxx, miny)  # Bottom right
    window_xrange_rs = maxx_px_rs - minx_px_rs
    window_yrange_rs = (maxy_px_rs - miny_px_rs)*-1  # *-1 because we have a negative cell size
    
    logger.info("        window_xrange_rs: {0}".format(window_xrange_rs))
    logger.info("        window_yrange_rs: {0}".format(window_yrange_rs))
    
    target_dataset = mem_drv.Create('', window_xrange_rs, window_yrange_rs, 1, data_band_type)
    
    # Set the geotransform
    #logger.info('resample geotransform')
    #logger.info(new_geo)
    target_dataset.SetGeoTransform( new_geo )
    target_dataset.SetProjection ( target_srs.ExportToWkt() )
    
    # Perform the projection/resampling
    logger.info("        reshaping MHHW raster to match window and cellsize...")
    res = gdal.ReprojectImage( src_window_dataset, target_dataset, src_srs.ExportToWkt(), target_srs.ExportToWkt(), respample_method, maxmem )
    logger.info("          done.")
    
    return target_dataset
