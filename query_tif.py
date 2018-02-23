#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-23 11:34
#

"""

"""

# -*- coding: utf-8 -*-
import json
import os, sys
from elasticsearch import Elasticsearch

from netcdftime import utime
import ogr, osr
import rasterio
from rasterio.features import geometry_mask
from affine import Affine
from databox.geomtrans import GeomTrans
from databox.netcdf4 import get_ncfile_handler, get_filter_files
import numpy as np
import numpy.ma as ma

from .CacheManager import lru_cache
from .gridtools import get_grid_by_xy, map_bbox_win, adjust_bbox, crs_to_proj4
from .gridtools import get_grids_by_bbox, get_grid_bbox, bbox_polygon

try:
    import cPickle as pickle
except:
    import pickle

EPSG_4326 = "EPSG:4326"

GEOM_MAX_SIZE = 8 * 1024 * 1024


class EDatabox(Exception):
    def __init__(self, code, reason):
        self.code = code
        self.reason = reason


class EInvalidBBox(EDatabox):
    def __init__(self, *args):
        EDatabox.__init__(self, -1, "Invalid BBox")


class EInvalidGeom(EDatabox):
    def __init__(self, *args):
        EDatabox.__init__(self, -2, "Invalid Geometry")


class EGeomTooLarge(EDatabox):
    def __init__(self, *args):
        EDatabox.__init__(self, -3, "Geometry too large")


class ETimeSlice(EDatabox):
    def __init__(self, *args):
        EDatabox.__init__(self, -4, " ".join(args))


def _ndobject_to_str(o):
    if isinstance(o, str):
        return o
    f = getattr(o, "tolist", None)
    if f: return f()
    return str(o)


class DataBoxQuery(object):
    def __init__(self, root, gsize):
        self.root = root
        self.gsize = gsize

    def _get_ncfile(self, sensor, grid_y, grid_x, bandid):
        ncfile = os.path.join(self.root, "%s/%s/%s/%s/%s/%s.nc" % (
            sensor, grid_y // 256, grid_y % 256, grid_x // 256, grid_x % 256, bandid))
        return ncfile

    def _get_ncfile_path(self, sensor, grid_y, grid_x):
        ncfile = os.path.join(self.root,
                              "%s/%s/%s/%s/%s" % (sensor, grid_y // 256, grid_y % 256, grid_x // 256, grid_x % 256))
        return ncfile

    @lru_cache(maxsize=256, timeout=300, args_base=1)
    def info_by_bbox(self, minx, miny, maxx, maxy, start_time, end_time, fmt="json"):
        '''
        返回空间范围所覆盖的数据切片信息，，返回：bands, crs, bbox, res, size, nctimes, geometry
        minx, miny, maxx, maxy：bbox范围, 对应投影信息默认为：EPSG:4326
        times：TimeSlice 可识别的时间条件
        '''
        start_time = start_time
        end_time = end_time
        query_polygon = {'type': 'Polygon', 'coordinates': [
            [[minx, maxy], [maxx, maxy], [maxx, miny], [minx, miny], [minx, maxy]]]}
        self.info_by_geom(query_polygon, start_time, end_time, fmt)

    @lru_cache(maxsize=256, timeout=300, args_base=1)
    def info_by_geom(self, geom, start_time, end_time, fmt="json"):
        '''
        返回空间范围所覆盖的数据切片信息，，返回：bands, crs, bbox, res, size, nctimes, geometry
        sensor：数据产品名称
        geom：矢量掩膜范围，支持 wkt 或 geojson, 对应投影信息默认为：EPSG:4326
        '''
        es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])

        query_polygon = {'type': 'Polygon', 'coordinates': [
            [[112.180, 22.367], [112.629, 22.383], [112.709, 21.965], [112.196, 21.9], [112.180, 22.367]]]}
        start_time = "20010117"
        end_time = "20150101"

        # query conditions include ['tile_path', 'wgs_grid', 'date_acquired', 'zone', 'wgs_crs', 'dataid', 'bandid', 'utm_grid', 'sensor', 'utm_crs',  'gridid']
        # bandid = bandid
        # sensor = sensor
        # query_polygon = query_polygon
        # start_time = start_time
        # end_time = end_time

        st_filter = {
            "query": {
                "bool": {
                    "filter": [
                        {"geo_shape": {
                            "wgs_grid": {
                                "shape": query_polygon,
                                "relation": "intersects"
                            }
                        }},
                        {'range': {'date_acquired': {'gt': start_time, 'lte': end_time, "format": "yyyyMMdd||yyyy"}}}

                    ]
                }
            }
        }

        query = json.dumps(st_filter)

        res = es.search(index="landsat_tiles", body=query, size=1000)
        print(res['hits']['total'])
        tile_dict = (res['hits']['hits'])

        for grid in tile_dict:
            print(grid['_source']['tile_path'], grid['_source']['wgs_grid'])
            print(grid['_source'].keys())
            print(grid['_source']['date_acquired'])

        return tile_dict

    @lru_cache(maxsize=256, timeout=300, args_base=1)
    def info_by_point(self, x, y, start_time, end_time, fmt="json"):
        '''
        返回坐标点位置的数据切片信息，返回：bands, crs, bbox, res, size, nctimes
        sensor：数据产品名称
        x, y：坐标, 对应投影信息默认为：EPSG:4326
        '''
        es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])
        x = 112
        y = 22
        query_point = {'type': 'Point', 'coordinates': [x, y]}
        start_time = "20010117"
        end_time = "20150101"
        st_filter = {
            "query": {
                "bool": {
                    "filter": [
                        {"geo_shape": {
                            "wgs_grid": {
                                "shape": query_point,
                                "relation": "contains"
                            }
                        }},
                        {'range': {'date_acquired': {'gt': start_time, 'lte': end_time, "format": "yyyyMMdd||yyyy"}}}

                    ]
                }
            }
        }

        query = json.dumps(st_filter)

        res = es.search(index="landsat_tiles", body=query, size=1000)
        print(res['hits']['total'])
        tile_dict = (res['hits']['hits'])

        for grid in tile_dict:
            print(grid['_source']['tile_path'], grid['_source']['wgs_grid'])
            print(grid['_source'].keys())
            print(grid['_source']['date_acquired'])
        return tile_dict

    @lru_cache(maxsize=256, timeout=300, args_base=1)
    def query_by_point(self, tif_file, x, y, fmt="json"):
        '''
        获取坐标点的数据。
        tif_file    ：数据产品
        bandid   ：波段名称
        x, y：坐标,坐标对应投影信息默认为：EPSG:4326
        '''
        pass

    def query_by_geom(self, wgs_geometry, tif_file, fmt="json"):
        '''
        获取空间范围内的数据，在调用该函数之前先调用 info_by_geom 或 info_by_bbox，将返回的结果中的 geometry 和 xy 属性作为参数。

        geom_info:矢量掩膜范围，必须包含 geometry 和 xy, geometry 必须为经纬度的 wkt 或 geojson，例如：
            {
                'wgs_geometry': 'POLYGON ((115.0 40.222408112063,115.0 40.3643188487053,115.312252122152 40.5,115.5 40.5,115.5 40.3046486467168,115.387540328212 40.2805104337424,115.0 40.222408112063))',
                'xy': [590, 260],
            }

        '''
        if len(wgs_geometry) > GEOM_MAX_SIZE:
            raise EGeomTooLarge()

        return self._query_by_geom(wgs_geometry, tif_file)

    @lru_cache(maxsize=256, timeout=300, args_base=1)
    def _query_by_geom(self, wgs_geometry, tif_file):
        raster = rasterio.open(tif_file, 'r')
        if raster is None:
            print("Failed to open file: " + tif_file)
            sys.exit()
        raster_proj = raster.crs.wkt

        # transform geographic coordinates of monitor points into projected coordinate system
        inSpatialRef = osr.SpatialReference()
        inSpatialRef.SetFromUserInput("EPSG:4326")
        outSpatialRef = osr.SpatialReference()
        outSpatialRef.SetFromUserInput(raster_proj)
        transform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

        wgs_geometry.Transform(transform)

        minx, maxx, miny, maxy = wgs_geometry.GetEnvelope()

        # get pixel coordinates of the geometry's bounding box,
        ll = raster.index(minx, miny)  # lowerleft bounds[0:2] xmin, ymin
        ur = raster.index(maxx, maxy)  # upperright bounds[2:4] xmax, ymax

        # when the shapefile polygon is larger than the raster
        row_begin = ur[0] if ur[0] > 0 else 0
        row_end = ll[0] + 1 if ll[0] > -1 else 0
        col_begin = ll[1] if ll[1] > 0 else 0
        col_end = ur[1] + 1 if ur[1] > -1 else 0
        window = ((row_begin, row_end), (col_begin, col_end))

        out_data = raster.read(window=window)

        # check whether the numpy array is empty or not?
        if out_data.size == 0 or np.all(out_data == raster.nodata):
            print('the grid does not intersect with the raster')
            return

        # create an affine transform for the subset data
        t = raster.transform
        shifted_affine = Affine(t.a, t.b, t.c + col_begin * t.a, t.d, t.e, t.f + row_begin * t.e)

        out_data = raster.read(window=window)

        # check whether the numpy array is empty or not?
        if out_data.size == 0 or np.all(out_data == raster.nodata):
            print('the grid does not intersect with the raster')
            return

        ret = {
            "nodata": float(raster.nodata),
            "width": out_data.shape[2],
            "height": out_data.shape[1],
            "crs": raster.crs,
            "transform": shifted_affine
        }
        ret["values"] = out_data

        out_bytes = pickle.dumps(ret)
        return out_bytes, "bytes"
