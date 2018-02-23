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

if __name__ == '__main__':
    # spatial_temporal_query_test()
    point_temporal_query_test()

