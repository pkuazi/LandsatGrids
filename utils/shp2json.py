#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-08 09:34
#

"""
store the grids of each zone into json for inserting into ElasticSearch
""" 
import osr, ogr
import os, fiona
import json
import numpy as np
from geomtrans import GeomTrans

class Shp2Json:
    def __init__(self, shapefile):
        self.shapefile = shapefile

    def shp2json(self):
        vector = fiona.open(self.shapefile, 'r')
        geomjson_list = []
        for feature in vector:
            geomjson_list.append(feature)

    def shp2json_fiona(self):
        vector = fiona.open(self.shapefile, 'r')
        geomjson_list = []
        for feature in vector:
            # create a shapely geometry
            # this is done for the convenience for the .bounds property only
            # feature['geoemtry'] is in Json format
            geojson = feature['geometry']
            geomjson_list.append(geojson)
        return geomjson_list

    def shp2json_ogr(self):
        dr = ogr.GetDriverByName("ESRI Shapefile")
        shp_ds = dr.Open(self.shapefile)
        layer = shp_ds.GetLayer(0)
        # shp_proj = layer.GetSpatialRef()
        # shp_proj4 = shp_proj.ExportToProj4()
        # extent = layer.GetExtent()  # minx, maxx, miny,  maxy
        geomjson_list = []
        feat_num = layer.GetFeatureCount()
        for i in range(feat_num):
            feat = layer.GetFeature(i)
            geom = feat.GetGeometryRef()
            geojson = json.loads(geom.ExportToJson())
            geomjson_list.append(geojson)
        return geomjson_list

if __name__ == '__main__':
    data_path = '/root/workspace/databox/CasGridEngine/griddata/grid_example/grid_addid'
    china_zone = range(40, 61)

    for zone in china_zone:
        shpfile = os.path.join(data_path, 'wgs_grid_%s.shp' % zone)

        vector = fiona.open(shpfile, 'r')
        json_list = []
        for feature in vector:
            json_list.append(feature)

        json_file = os.path.join('/root/workspace/databox/CasGridEngine/griddata/grid_example/grid_json', 'wgs_grid_%s.txt'%zone)
        with open(json_file, 'w') as outfile:
            json.dump(json_list, outfile)

