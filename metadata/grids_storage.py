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
import ogr
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
                "utm_geometry": {"type": "text"}
            }
        }
    }
}
mapping = json.dumps(mapping)
print(mapping)


def shape_to_elasticsearch(shapefile, doc):
    ds = ogr.OpenShared(shapefile)
    if ds is None:
        return

    layer = ds.GetLayerByIndex(0)
    sr = layer.GetSpatialRef()
    proj4 = sr.ExportToProj4()
    for feat in layer:
        geojson = feat.ExportToJson(as_object=True)
        del geojson["id"]
        del geojson["type"]
        geojson.update(geojson["properties"])
        del geojson["properties"]

        geojson_l = {}
        for key in geojson.keys():
            geojson_l[key.lower()] = geojson[key]
        geojson_l["crs"] = proj4

        print(geojson_l)

        es.index(index=index_name, doc_type="grids", body=geojson_l)


def wgs_utm_into_es(wgs_shp, utm_shp, zone):
    wgs_ds = ogr.OpenShared(wgs_shp)
    if wgs_ds is None:
        return
    utm_ds = ogr.OpenShared(utm_shp)
    if utm_ds is None:
        return

    wgs_layer = wgs_ds.GetLayerByIndex(0)
    utm_layer = utm_ds.GetLayerByIndex(0)

    wgs_sr = wgs_layer.GetSpatialRef()
    wgs_proj4 = wgs_sr.ExportToProj4()
    utm_sr = utm_layer.GetSpatialRef()
    utm_proj4 = utm_sr.ExportToProj4()

    for wgs_feat in wgs_layer:
        geojson_l = {}

        geojson_l["zone"] = zone
        geojson_l['wgs_crs'] = wgs_proj4
        geojson_l['utm_crs'] = utm_proj4

        featjson = wgs_feat.ExportToJson(as_object=True)
        geojson_l['gridid'] = featjson['properties']['GridID']

        line_form_coordinates = featjson['geometry']['coordinates'][0]
        line_form_coordinates.append(featjson['geometry']['coordinates'][0][0])
        geojson_l["wgs_geometry"] = {'type': featjson['geometry']['type'], 'coordinates': [line_form_coordinates]}

        # wgs_geom = wgs_feat.GetGeometryRef()
        # print(wgs_geom.GetGeometryType())
        # if (wgs_geom.GetGeometryType() == ogr.wkbPolygon):
        #     geojson_l["wgs_geometry"] = wgs_geom

        # geojson_l["wgs_geometry"] = featjson['geometry']

        for utm_feat in utm_layer:
            utm_feat_json = utm_feat.ExportToJson(as_object=True)
            if utm_feat_json['properties']['GridID'] == geojson_l['gridid']:
                line_form_coordinates = utm_feat_json['geometry']['coordinates'][0]
                line_form_coordinates.append(utm_feat_json['geometry']['coordinates'][0][0])
                geojson_l["wgs_geometry"] = {'type': utm_feat_json['geometry']['type'], 'coordinates': [line_form_coordinates]}

                # geojson_l['utm_geometry'] = utm_feat.GetGeometryRef()
                break

        print(geojson_l)
        es.index(index=index_name, doc_type="grids", body=geojson_l)


if __name__ == '__main__':
    index_name = "grids"
    try:
        es.indices.delete(index=index_name)
    except:
        pass

    es.indices.create(index=index_name, body=mapping)

    data_path = '/mnt/win/L45grids'

    china_zone = range(40, 61)
    for zone in china_zone:
        wgs_shp = os.path.join(data_path, 'wgs_grid_%s.shp' % zone)
        utm_shp = os.path.join(data_path, 'utm_grid_%s.shp' % zone)
        wgs_utm_into_es(wgs_shp, utm_shp, zone)
