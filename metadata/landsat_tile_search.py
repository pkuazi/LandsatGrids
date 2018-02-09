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
es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])


if __name__ == '__main__':
    query_polygon = {'type':'Polygon', 'coordinates':[[[112.180,22.367], [112.629,22.383], [112.709,21.965], [112.196,21.9], [112.180,22.367]]] }
    filter = {
        "query":{
            "bool": {
                "filter": {
                    "geo_shape": {
                        "wgs_grid": {
                            "shape": query_polygon,
                            "relation": "intersects"
                        }
                    }
                }
            }
        }
    }

    query = json.dumps(filter)

    res = es.search(index="landsat_tiles", body=query, size=1000)
    print(res['hits']['total'])
    tile_dict = (res['hits']['hits'])

    for grid in tile_dict:
        print(grid['_source']['tile_path'], grid['_source']['wgs_grid'])

