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

# Import osgeo
from osgeo import osr
from osgeo import gdal
from osgeo import ogr
from osgeo import gdalconst

def World2PixelCoords(gdal_geotransform, world_x, world_y):
    """ Convert from world coordinates to pixel coordinates using the GDAL geotransform from a raster """
    # Get data from geotransform
    #   gdal_geotransform
    #     origin_x, e-w resolution, rotation, origin_y, rotation, n-s resoluton (usually negative)
    origin_x = gdal_geotransform[0]
    ew_resoltion = gdal_geotransform[1]
    origin_y = gdal_geotransform[3]
    nw_resolution = gdal_geotransform[5]
    
    # Do the conversions
    pixel_x = int((world_x - origin_x)/ew_resoltion)
    pixel_y = int((world_y - origin_y)/(nw_resolution))
    return (pixel_x, pixel_y)
    
def Pixel2WorldCoords(gdal_geotransform, pixel_x, pixel_y):
    """ Convert from pixel coordinates to world coordinates using the GDAL geotransform from a raster """
    
    # Get data from geotransform
    #   gdal_geotransform
    #     origin_x, e-w resolution, rotation, origin_y, rotation, n-s resoluton (usually negative)
    #
    origin_x = gdal_geotransform[0]
    ew_resoltion = gdal_geotransform[1]
    origin_y = gdal_geotransform[3]
    nw_resolution = gdal_geotransform[5]
    
    # Do the conversions
    world_x =  (pixel_x*ew_resoltion)+origin_x
    world_y = (pixel_y*nw_resolution)+origin_y
    
    return (world_x, world_y)

def CopyBand( srcband, dstband ):
    """
    CopyBand is from gdal_fillnodata.py
    
    Project:  GDAL Python Interface
    Purpose:  Application for filling nodata areas in a raster by interpolation
    Author:   Frank Warmerdam, warmerdam@pobox.com
 
    Copyright (c) 2008, Frank Warmerdam
    # 
    #  Permission is hereby granted, free of charge, to any person obtaining a
    #  copy of this software and associated documentation files (the "Software"),
    #  to deal in the Software without restriction, including without limitation
    #  the rights to use, copy, modify, merge, publish, distribute, sublicense,
    #  and/or sell copies of the Software, and to permit persons to whom the
    #  Software is furnished to do so, subject to the following conditions:
    # 
    #  The above copyright notice and this permission notice shall be included
    #  in all copies or substantial portions of the Software.
    # 
    #  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
    #  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    #  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    #  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    #  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    #  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    #  DEALINGS IN THE SOFTWARE.
    #******************************************************************************
    
    """
    for line in range(srcband.YSize):
        line_data = srcband.ReadRaster( 0, line, srcband.XSize, 1 )
        dstband.WriteRaster( 0, line, srcband.XSize, 1, line_data, buf_type = srcband.DataType )