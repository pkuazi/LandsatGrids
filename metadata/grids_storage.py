#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-08 14:11
#

"""
write wgs_grids and utm_grids into elasticsearch
"""
from elasticsearch import Elasticsearch
import os
import ogr, osr
import json

es = Elasticsearch(hosts=[{"host": "10.0.138.156", "port": 9200}, ])

mapping = {
    "mappings": {
        "grids": {
            "properties": {
                "gridid": {"type": "text"},
                "zone": {"type": "integer"},
                "wgs_crs": {"type": "text"},
                "utm_crs": {"type": "text"},
                "wgs_geometry": {"type": "geo_shape"},
                # "utm_geometry": {"type": "text"}
                "utm_geometry": {
                    "properties": {
                        "coordinates": {
                            "type": "float"
                        },
                        "type": {
                            "type": "text"
                        }
                    }
                },
            }
        }
    }
}

mapping = json.dumps(mapping)
print(mapping)

def utm_grid_into_es(utm_shp, zone):
    utm_ds = ogr.OpenShared(utm_shp)
    if utm_ds is None:
        print("shapefile not exists:", utm_shp)
        return

    utm_layer = utm_ds.GetLayerByIndex(0)

    wgs_proj4 = "+proj=longlat +datum=WGS84 +no_defs "
    wgs_sr = osr.SpatialReference()
    wgs_sr.SetFromUserInput(wgs_proj4)

    utm_sr = utm_layer.GetSpatialRef()
    utm_proj4 = utm_sr.ExportToProj4()

    transform = osr.CoordinateTransformation(utm_sr, wgs_sr)

    # utm_objs = {}
    for utm_feat in utm_layer:
        utm_feat_json = utm_feat.ExportToJson(as_object=True)
        geom_properties = utm_feat_json['properties']
        gridid_ = geom_properties['GridID']

        utm_geom = utm_feat.geometry()
        wgs_geom = utm_geom.Clone()
        wgs_geom.Transform(transform)

        utm_geom_json = json.loads(utm_geom.ExportToJson())
        utm_geom_json["coordinates"][0].append(utm_geom_json["coordinates"][0][0])

        wgs_geom_json = json.loads(wgs_geom.ExportToJson())
        wgs_geom_json["coordinates"][0].append(wgs_geom_json["coordinates"][0][0])

        geojson_l = {}
        geojson_l["zone"] = zone
        geojson_l['wgs_crs'] = wgs_proj4
        geojson_l['utm_crs'] = utm_proj4

        geojson_l['gridid'] = gridid_
        geojson_l["wgs_geometry"] = wgs_geom_json
        geojson_l["utm_geometry"] = utm_geom_json

        print(geojson_l)
        es.index(index=index_name, doc_type="grids", body=geojson_l)


if __name__ == '__main__':
    index_name = "utm_grids_100km"
    try:
        es.indices.delete(index=index_name)
    except:
        pass

    es.indices.create(index=index_name, body=mapping)

    data_path = '/mnt/win/L45grids'

    china_zone = range(40, 61)
    for zone in china_zone:
        # wgs_shp = os.path.join(data_path, 'wgs_grid_%s.shp' % zone)
        utm_shp = os.path.join(data_path, 'utm_grid_%s.shp' % zone)
        utm_grid_into_es(utm_shp, zone)
