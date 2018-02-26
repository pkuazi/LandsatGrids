#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-09 15:52
#

"""
store metadata of the landsat tiles clipped by grids
"""

from elasticsearch import Elasticsearch
import json
import os
import rasterio
from grids_search import query_grids, mask_image_by_geometry
from geomtrans import GeomTrans

es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])

mapping = {
    "mappings": {
        "landsat45_tiles": {
            "properties": {
                "dataid":{"type":"text"},
                "bandid":{"type":"text"},
                "date_acquired":{"type":"date"},
                "sensor":{"type":"text"},
                "tile_path":{"type":"text"},
                "gridid": {"type": "text"},
                "zone": {"type": "integer"},
                "wgs_crs": {"type": "text"},
                "utm_crs": {"type": "text"},
                "wgs_grid": {"type": "geo_shape"},
                "utm_grid": {"properties": {
                        "coordinates": {
                            "type": "float"
                        },
                        "type": {
                            "type": "text"
                        }
                    }}
            }
        }
    }
}
mapping = json.dumps(mapping)
print(mapping)

def main(file):
    # dataid for metadata
    items = file.split("/")
    dataid = items[-1].split(".")[0]
    bandname = 'B0'+dataid[-1:]

    time = dataid[14:22]
    sensor = dataid[:2]
    row = int(dataid[6:9])
    path = int(dataid[10:13])
    year = int(time[:4])

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
        # tile_path = os.path.join("/tmp", dataid + '_' + grid['_source']['gridid'] + '.tif')

        tile_path = "/LANDSAT/L45TM/%s/%s/%s/%s/%s" % (row, path, year, time, grid['_source']['gridid'])
        tile_file = os.path.join(tile_path, dataid + '_' + grid['_source']['gridid'] + '.tif')

        mask_image_by_geometry(utm_geometry, raster, tile_file)

        # metadata for the tile clipped from landsat by a grid
        geojson_l = {}
        geojson_l['dataid']=dataid
        geojson_l['bandid']=bandname
        geojson_l["date_acquired"] = time
        geojson_l["sensor"] = sensor
        geojson_l['tile_path']=tile_path
        geojson_l['gridid'] = grid['_source']['gridid']
        geojson_l['zone'] = grid['_source']['zone']
        geojson_l['wgs_crs'] = grid['_source']['wgs_crs']
        geojson_l['utm_crs'] = grid['_source']['utm_crs']
        geojson_l['wgs_grid'] = grid['_source']['wgs_geometry']
        print(geojson_l['wgs_grid'])
        geojson_l['utm_grid'] = grid['_source']['utm_geometry']

        # print(geojson_l)

        es.index(index=index_name, doc_type="landsat45_tiles", body=geojson_l)




if __name__ == '__main__':
    index_name = "landsat_tiles"
    try:
        es.indices.delete(index=index_name)
    except:
        pass

    es.indices.create(index=index_name, body=mapping)

    data_path = '/mnt/win/L45images'
    # unzip all the compressed files before
    onlyfiles = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]

    for file in onlyfiles:
        if file.endswith('.TIF') or file.endswith('.tif'):
            file = os.path.join(data_path, file)
            print(file)
            main(file)

