#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-09 16:28
#

"""
spatial query of landsat tiles
"""
from elasticsearch import Elasticsearch
import json
from geomtrans import GeomTrans

es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])

def spatial_temporal_query_test():
    query_polygon = {'type': 'Polygon', 'coordinates': [
        [[112.180, 22.367], [112.629, 22.383], [112.709, 21.965], [112.196, 21.9], [112.180, 22.367]]]}
    start_time = "20010117"
    end_time = "20150101"
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
                    {'range':{'date_acquired':{'gt':start_time, 'lte':end_time, "format":"yyyyMMdd||yyyy"}}}

                ]
            }
        }
    }

    # filter = {
    #     "query": {
    #         "bool": {
    #             "filter": {
    #                 "geo_shape": {
    #                     "wgs_grid": {
    #                         "shape": query_polygon,
    #                         "relation": "intersects"
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

    # query = json.dumps(filter)
    query = json.dumps(st_filter)

    res = es.search(index="landsat_tiles", body=query, size=1000)
    print(res['hits']['total'])
    tile_dict = (res['hits']['hits'])

    for grid in tile_dict:
        print(grid['_source']['tile_path'], grid['_source']['wgs_grid'])
        print(grid['_source'].keys())
        print(grid['_source']['date_acquired'])

def point_temporal_query_test():
    query_point = {'type': 'Point', 'coordinates': [112, 22]}
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
                    {'range':{'date_acquired':{'gt':start_time, 'lte':end_time, "format":"yyyyMMdd||yyyy"}}}

                ]
            }
        }
    }

    # filter = {
    #     "query": {
    #         "bool": {
    #             "filter": {
    #                 "geo_shape": {
    #                     "wgs_grid": {
    #                         "shape": query_polygon,
    #                         "relation": "intersects"
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

    # query = json.dumps(filter)
    query = json.dumps(st_filter)

    res = es.search(index="landsat_tiles", body=query, size=1000)
    print(res['hits']['total'])
    tile_dict = (res['hits']['hits'])

    for grid in tile_dict:
        print(grid['_source']['tile_path'], grid['_source']['wgs_grid'])
        print(grid['_source'].keys())
        print(grid['_source']['date_acquired'])

import rasterio, sys, osr
def mask_image_by_geometry( wgs_geometry, tif_file):
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
    import numpy as np
    if out_data.size == 0 or np.all(out_data == raster.nodata):
        print('the grid does not intersect with the raster')
        return

    return out_data

if __name__ == '__main__':
    # spatial_temporal_query_test()
    # point_temporal_query_test()
    raster ="/mnt/win/L45images/L5-TM-121-044-19950105-LSR-B1.TIF"

    import ogr
    wgs_grids = "/mnt/win/L45grids/wgs_grid_50.shp"
    ds = ogr.OpenShared(wgs_grids)

    layer = ds.GetLayerByIndex(0)

    feat = layer.GetFeature(476)
    wgs_geometry = feat.GetGeometryRef()

    mask_image_by_geometry(wgs_geometry, raster)

