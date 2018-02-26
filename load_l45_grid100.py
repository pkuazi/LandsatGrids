#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-23 09:37
#

"""
store metadata of the landsat tiles clipped by grids
"""

from elasticsearch import Elasticsearch
import json
import os
import rasterio
import numpy as np
from shapely.geometry import shape
from affine import Affine
from geomtrans import GeomTrans

ES_host = "10.0.138.156"
ES_port = 9200
tile_index_name = "landsat_tiles"
grid_index_name = "utm_grids_100km"

# es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])

mapping = {
    "mappings": {
        "landsat45_tiles": {
            "properties": {
                "dataid": {"type": "text"},
                "bandid": {"type": "text"},
                "date_acquired": {"type": "date", "format": "yyyyMMdd"},
                "sensor": {"type": "text"},
                "tile_path": {"type": "text"},
                "gridid": {"type": "text"},
                "zone": {"type": "integer"},
                "wgs_crs": {"type": "text"},
                "utm_crs": {"type": "text"},
                "wgs_grid": {"type": "geo_shape"},
                "utm_grid": {
                    "properties": {
                        "coordinates": {
                            "type": "float"
                        },
                        "type": {
                            "type": "text"
                        }
                    }
                }
            }
        }
    }
}
mapping = json.dumps(mapping)
print(mapping)


class MetaDB(object):
    def __init__(self, host, port):
        self.es = Elasticsearch(hosts=[{"host": host, "port": port}, ])

    def insert_tilemeta(self, index_name, doc_type, dataid, bandid, ctime, sensor, tile_file, gridid, utm_zone, wgs_crs,
                        utm_crs, wgs_grid, utm_grid):
        # es = Elasticsearch(hosts=[{"host": self.host, "port": self.port}, ])
        # metadata for the tile clipped from landsat by a grid
        geojson_l = {}
        geojson_l['dataid'] = dataid
        geojson_l['bandid'] = bandid
        geojson_l["date_acquired"] = ctime
        geojson_l["sensor"] = sensor
        geojson_l['tile_path'] = tile_file
        geojson_l['gridid'] = gridid
        geojson_l['zone'] = utm_zone
        geojson_l['wgs_crs'] = wgs_crs
        geojson_l['utm_crs'] = utm_crs
        geojson_l['wgs_grid'] = wgs_grid
        print(geojson_l['wgs_grid'])
        geojson_l['utm_grid'] = utm_grid

        # print(geojson_l)
        self.es.index(index=index_name, doc_type=doc_type, body=geojson_l)

        # def commit(self):
        #     self.db.commit()
    def delete(self, index_name):
        self.es.indices.delete(index=index_name)
    def create(self, index_name, mapping):
        self.es.indices.create(index=index_name, body=mapping)

class GridUnitBuilder(object):
    def __init__(self, raster_file, grid_root, product, sensor, ctime, dataid, bandid, row, path, year, gsize):
        self.raster_file = raster_file
        self.grid_root = grid_root
        self.product = product.upper()
        self.sensor = sensor.upper()
        self.ctime = ctime
        self.dataid = dataid
        self.bandid = bandid.upper()
        self.gsize = gsize

        self.row = row
        self.path = path
        self.year = year

        self.metadb = MetaDB(ES_host, ES_port)

        print("process:", raster_file)

    def _get_tile(self, dataid, gridid):
        tile_path = self.grid_root + "/%s/%s/%s/%s/%s/%s/%s" % (self.product, self.sensor, self.row, self.path, self.year, self.ctime, gridid)
        tile_file = os.path.join(tile_path, dataid + '_' + gridid + '.tif')
        return tile_file

    def search_grids(self, grid_index_name, utm_zone, wgs_boundary):
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

        res = self.metadb.es.search(index=grid_index_name, body=query, size=1000)
        # print(res['hits']['total'])
        grid_dict = (res['hits']['hits'])

        # for grid in grid_dict:
        #     print(grid['_source']['gridid'])
        return grid_dict

    def mask_image_by_geometry(self, utm_geometry, raster, tile_file):
        # geomshape = ast.literal_eval(geomjson)
        geomshape = utm_geometry
        geometry = shape(geomshape)
        # get pixel coordinates of the geometry's bounding box,
        ll = raster.index(*geometry.bounds[0:2])  # lowerleft bounds[0:2] xmin, ymin
        ur = raster.index(*geometry.bounds[2:4])  # upperright bounds[2:4] xmax, ymax

        # when the shapefile polygon is larger than the raster
        row_begin = ur[0] if ur[0] > 0 else 0
        row_end = ll[0] + 1 if ll[0] > -1 else 0
        col_begin = ll[1] if ll[1] > 0 else 0
        col_end = ur[1] + 1 if ur[1] > -1 else 0
        window = ((row_begin, row_end), (col_begin, col_end))

        # create an affine transform for the subset data
        t = raster.transform
        shifted_affine = Affine(t.a, t.b, t.c + col_begin * t.a, t.d, t.e, t.f + row_begin * t.e)

        out_data = raster.read(window=window)
        print(tile_file, out_data.size)

        # check whether the numpy array is empty or not?
        if out_data.size == 0 or np.all(out_data == raster.nodata):
            print('the grid does not intersect with the raster')
            return

        with rasterio.open(tile_file, 'w', driver='GTiff', width=out_data.shape[2], height=out_data.shape[1],
                           crs=raster.crs, transform=shifted_affine, nodata=raster.nodata, count=1,
                           dtype=rasterio.int16) as dst:
            dst.write(out_data)

    def save_tile_by_grid(self):
        # query condition: zone of utm + bbox of wgs84
        raster = rasterio.open(self.raster_file, 'r')
        utm_zone = raster.crs['init'][-2:]

        bbox = raster.bounds
        utm_extent = [[bbox.left, bbox.top], [bbox.left, bbox.bottom], [bbox.right, bbox.bottom],
                      [bbox.right, bbox.top],
                      [bbox.left, bbox.top]]

        utm2wgs = GeomTrans(str(raster.crs.wkt), 'EPSG:4326')
        wgs_extent = utm2wgs.transform_points(utm_extent)
        wgs_boundary = {'coordinates': [wgs_extent], 'type': 'Polygon'}

        grid_dicts = self.search_grids(grid_index_name, utm_zone, wgs_boundary)

        for grid in grid_dicts:
            gridid = grid['_source']['gridid']
            utm_geometry = grid['_source']['utm_geometry']

            tile_file = self._get_tile(self.dataid, gridid)

            tile_path = os.path.dirname(tile_file)
            if not os.path.exists(tile_path):
                os.makedirs(tile_path)

            self.mask_image_by_geometry(utm_geometry, raster, tile_file)

            utm_zone = grid['_source']['zone']
            wgs_crs = grid['_source']['wgs_crs']
            utm_crs = grid['_source']['utm_crs']
            wgs_grid = grid['_source']['wgs_geometry']
            utm_grid = grid['_source']['utm_geometry']
            self.metadb.insert_tilemeta(tile_index_name, "landsat45_tiles", self.dataid, self.bandid, self.ctime,
                                        self.sensor, tile_file, gridid, utm_zone, wgs_crs, utm_crs, wgs_grid, utm_grid)

def process(file, root):
    # dataid for metadata
    items = file.split("/")
    product = "LANDSAT"
    dataid = items[-1].split(".")[0]
    bandid = 'B0' + dataid[-1:]

    ctime = dataid[14:22]
    sensor = dataid[:2]
    row = int(dataid[6:9])
    path = int(dataid[10:13])
    year = int(ctime[:4])
    gsize = 100

    gub = GridUnitBuilder(file,root, product, sensor, ctime, dataid, bandid, row, path, year, gsize)
    gub.save_tile_by_grid()


def main():
    data_path = '/mnt/win/L45images'
    root = "/mnt/LandsatGrids"
    # unzip all the compressed files before
    onlyfiles = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]

    for file in onlyfiles:
        if file.endswith('.TIF') or file.endswith('.tif'):
            file = os.path.join(data_path, file)
            process(file, root)


if __name__ == '__main__':
    es = MetaDB(ES_host, ES_port)
    try:
        es.delete(tile_index_name)
    except:
        pass

    es.create(tile_index_name, mapping)

    # es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])
    # try:
    #     es.indices.delete(index=tile_index_name)
    # except:
    #     pass
    #
    # es.indices.create(index=tile_index_name, body=mapping)

    main()
