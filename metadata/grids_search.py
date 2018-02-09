#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-08 17:10
#

"""
find grids for each landsat45 image and clip
"""
from elasticsearch import Elasticsearch
import os, sys
import ast
import json
import rasterio
from shapely.geometry import shape
from geomtrans import GeomTrans
from affine import Affine
import numpy as np

es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])


def query_grids(utm_zone, wgs_boundary):
    #     filter = {
    #     "query":{
    #         "bool": {
    #             "must": {
    #                 "match_all": {}
    #             },
    #             "filter": {
    #                 "geo_shape": {
    #                     "wgs_geometry": {
    #                         "shape": wgs_boundary,
    #                         "relation": "intersects"
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

    filter = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {
                        "zone": utm_zone
                    }},
                    {"geo_shape": {
                        "wgs_geometry": {
                            "shape": wgs_boundary,
                            "relation": "intersects"
                        }
                    }}
                ]
            }
        }
    }
    query = json.dumps(filter)

    res = es.search(index="grids", body=query, size=1000)
    print(res['hits']['total'])
    grid_dict = (res['hits']['hits'])

    # for grid in grid_dict:
    #     print(grid['_source']['gridid'])
    return grid_dict


def mask_image_by_geometry(geomjson, raster, name):
    geomshape = ast.literal_eval(geomjson)
    geometry = shape(geomshape)
    # get pixel coordinates of the geometry's bounding box,
    ll = raster.index(*geometry.bounds[0:2])  # lowerleft bounds[0:2] xmin, ymin
    ur = raster.index(*geometry.bounds[2:4])  # upperright bounds[2:4] xmax, ymax

    # create an affine transform for the subset data
    # t = raster.transform
    # shifted_affine = Affine(t.a, t.b, t.c + ll[1] * t.a, t.d, t.e, t.f + ur[0] * t.e)
    # # read the subset of the data into a numpy array
    # window = ((ur[0], ll[0] + 1), (ll[1], ur[1] + 1))

    # when the shapefile polygon is larger than the raster
    row_begin = ur[0] if ur[0] > 0 else 0
    row_end = ll[0] + 1 if ll[0] > -1 else 0
    col_begin = ll[1] if ll[1] > 0 else 0
    col_end = ur[1] + 1 if ur[1] > -1 else 0
    window = ((row_begin, row_end),(col_begin, col_end))

    # create an affine transform for the subset data
    t = raster.transform
    shifted_affine = Affine(t.a, t.b, t.c + col_begin * t.a, t.d, t.e, t.f + row_begin * t.e)

    out_data = raster.read(window=window)
    print(name, out_data.size)

    # check whether the numpy array is empty or not?
    if out_data.size == 0 or np.all(out_data==raster.nodata):
        print('the grid does not intersect with the raster')
        return

    # with rasterio.open("/tmp/%s" % name, 'w', driver='GTiff', width=out_data.shape[2], height=out_data.shape[1],crs=raster.crs,transform=shifted_affine, dtype=rasterio.uint16, nodata=256, count=raster.count,indexes=raster.indexes) as dst:
        # Write the src array into indexed bands of the dataset. If `indexes` is a list, the src must be a 3D array of matching shape. If an int, the src must be a 2D array.
        # dst.write(out_data.astype(rasterio.uint16), indexes=raster.indexes)
    with rasterio.open("/tmp/%s" % name, 'w', driver='GTiff', width=out_data.shape[2], height=out_data.shape[1],
                           crs=raster.crs, transform=shifted_affine, nodata=raster.nodata, count=1, dtype= rasterio.int16) as dst:
        dst.write(out_data)


def main(file):
    # dataid for metadata
    items = file.split("/")
    dataid = items[-1].split(".")[0]

    # query condition: zone of utm + bbox of wgs84
    raster = rasterio.open(file, 'r')
    utm_zone = raster.crs['init'][-2:]

    bbox = raster.bounds
    utm_extent = [[bbox.left, bbox.top], [bbox.left, bbox.bottom], [bbox.right, bbox.bottom], [bbox.right, bbox.top],
                  [bbox.left, bbox.top]]

    utm2wgs = GeomTrans(str(raster.crs.wkt), 'EPSG:4326')
    wgs_extent = utm2wgs.transform_points(utm_extent)
    wgs_boundary = {'coordinates': [wgs_extent], 'type': 'Polygon'}

    grid_dicts = query_grids(utm_zone, wgs_boundary)

    for grid in grid_dicts:
        print(grid['_source']['gridid'])
        utm_geometry = grid['_source']['utm_geometry']
        mask_image_by_geometry(utm_geometry, raster, dataid + '_' + grid['_source']['gridid'] + '.tif')


if __name__ == '__main__':
    data_path = '/mnt/win/L45images'

    # unzip all the compressed files before
    onlyfiles = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]

    for file in onlyfiles:
        if file.endswith('.TIF') or file.endswith('.tif'):
            file = os.path.join(data_path, file)
            print(file)
            main(file)
