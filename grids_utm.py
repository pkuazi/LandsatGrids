#
# -*-coding:utf-8 -*-
#
# @Module:
#
# @Author: zhaojianghua
# @Date  : 2018-01-30 09:13
#

"""

""" 

#
# -*-coding:utf-8 -*-
#
# @Module:
#
# @Author: zhaojianghua
# @Date  : 2018-01-29 16:27
#

"""

"""

import osr, ogr


class GeomTrans(object):
    def __init__(self, in_proj, out_proj):
        self.transform = None

        if in_proj:
            self.inSpatialRef = osr.SpatialReference()
            self.inSpatialRef.SetFromUserInput(in_proj)
        else:
            return

        if out_proj:
            self.outSpatialRef = osr.SpatialReference()
            self.outSpatialRef.SetFromUserInput(out_proj)
        else:
            return

        if self.inSpatialRef.IsSame(self.outSpatialRef) == 0:
            self.transform = osr.CoordinateTransformation(self.inSpatialRef, self.outSpatialRef)

    def transform_point(self, point):
        if self.transform is None:
            return point

        geom = ogr.Geometry(ogr.wkbPoint)
        geom.AddPoint(point[0], point[1])
        geom.Transform(self.transform)

        return geom.GetX(), geom.GetY()

    def transform_points(self, points):
        return [self.transform_point(point) for point in points]

    def transform_geom(self, geometry):
        if geometry.find('{') >= 0:
            geom = ogr.CreateGeometryFromJson(geometry)
        else:
            geom = ogr.CreateGeometryFromWkt(geometry)

        if self.transform is not None:
            geom.Transform(self.transform)

        return geom

    def transform_wkt(self, geometry):
        return self.transform_geom(geometry).ExportToWkt()

    def transform_json(self, geometry):
        return self.transform_geom(geometry).ExportToJson()


from shapely.geometry import mapping, Polygon


def geojson2shp(geojson, shpdst, id):
    '''

    :param geojson: the geojson format of a polygon
    :param shpdst: the path of the shapefile
    :param id: the id property
    :return: no return, just save the shapefile into the shpdst
    '''
    # an example Shapely geometry
    coordinates = geojson['coordinates']
    poly = Polygon(coordinates[0])

    # Define a polygon feature geometry with one attribute
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }

    # Write a new Shapefile
    with fiona.open(shpdst, 'w', 'ESRI Shapefile', schema) as c:
        ## If there are multiple geometries, put the "for" loop here
        c.write({
            'geometry': mapping(poly),
            'properties': {'id': id},
        })


import json


class Shp2Json:
    def __init__(self, shapefile):
        self.shapefile = shapefile

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


import fiona

if __name__ == '__main__':
    shapefile = '/root/workspace/databox/CasGridEngine/griddata/grid_example/grid_50/grid_50.shp'
    vector = fiona.open(shapefile, 'r')

    utm50_wgs = '/root/workspace/databox/CasGridEngine/griddata/grid_example/grid_50/grid_50_wgs.shp'

    dr = ogr.GetDriverByName("ESRI Shapefile")
    shp_ds = dr.Open(utm50_wgs)
    layer = shp_ds.GetLayer(0)

    feat_num = layer.GetFeatureCount()

    china_zone = range(40, 61)
    for zone in china_zone:
        mv_degree = (zone - 50) * 6

        for i in range(feat_num):
            feat = layer.GetFeature(i)
            geom = feat.GetGeometryRef()
            # move geom by mv_degree: to geojson, move, to geometry
