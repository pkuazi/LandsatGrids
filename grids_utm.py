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
import fiona
import numpy as np
import os, json


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


def create_shapefile(shp_dst, geometry_list, proj):
    dr = ogr.GetDriverByName("ESRI Shapefile")
    ds = dr.CreateDataSource(shp_dst)

    sr = osr.SpatialReference()
    sr.SetFromUserInput(proj)

    lyr = ds.CreateLayer("polygon", sr, ogr.wkbPolygon)
    for geom in geometry_list:
        ffd = ogr.FeatureDefn()

        fgd = ogr.GeomFieldDefn()
        fgd.name = "id"
        fgd.type = ogr.wkbPolygon

        ffd.AddGeomFieldDefn(fgd)
        #
        # feat = ogr.Feature(ffd)
        # geom_p = ogr.Geometry(ogr.wkbPolygon)
        # geom_p.AddGeometry(geom)
        # feat.SetGeometry(geom_p)
        feat = ogr.Feature()
        feat.SetGeometry(geom)

        lyr.CreateFeature(feat)


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


if __name__ == '__main__':
    data_path = '/root/workspace/databox/CasGridEngine/griddata/grid_example'
    grid_50 = os.path.join(data_path, 'grid_50/grid_50.shp')

    vector = fiona.open(grid_50, 'r')
    in_proj = vector.crs_wkt
    wgs_proj = 'EPSG:4326'
    utm2wgs = GeomTrans(in_proj, wgs_proj)

    china_zone = range(40, 61)
    for zone in china_zone:
        dst_shp = os.path.join(data_path, 'wgs_grid_%s.shp' % zone)
        utm_proj = "EPSG:326%s" % (zone)
        print(utm_proj)
        wgs2utm = GeomTrans(wgs_proj, utm_proj)

        mv_degree = (zone - 50) * 6

        # move geom by mv_degree: to geojson, move, to geometry
        shp2json = Shp2Json(grid_50)
        json_list = shp2json.shp2json_fiona()

        # create the new shp
        dr = ogr.GetDriverByName("ESRI Shapefile")
        ds = dr.CreateDataSource(dst_shp)
        sr = osr.SpatialReference()
        sr.SetFromUserInput(wgs_proj)
        # sr.SetFromUserInput(utm_proj)
        lyr = ds.CreateLayer("polygon", sr, ogr.wkbPolygon)

        # geom_list = []
        for geo_json in json_list:
            corner_points = geo_json['coordinates'][0]
            # utm to wgs
            corner_points_wgs = utm2wgs.transform_points(corner_points)

            # delete those grids not between 80S to 84N
            points_array = np.array(corner_points_wgs)
            max_lat = np.max(points_array[:, 1])
            min_lat = np.min(points_array[:, 1])

            if max_lat > 84 or min_lat < -80:
                continue

            corner_points_moved = []
            for point in corner_points_wgs:
                # wgs move degree
                x = point[0] + mv_degree
                y = point[1]
                corner_points_moved.append((x, y))

            # wgs to utm
            # corner_points_new_utm = wgs2utm.transform_points(corner_points_moved)

            # write into one geometry
            # geo_json_moved = {'coordinates':[corner_points_new_utm], 'type':'Polygon'}
            # geo_json_moved = json.dumps(geo_json_moved)

            geo_json_moved = {'coordinates': [corner_points_moved], 'type': 'Polygon'}
            geo_json_moved = json.dumps(geo_json_moved)

            # geom_json = json.dumps(geom_json)
            geom = ogr.CreateGeometryFromJson(geo_json_moved)

            ffd = ogr.FeatureDefn()
            fgd = ogr.GeomFieldDefn()
            fgd.name = "id"
            fgd.type = ogr.wkbPolygon
            ffd.AddGeomFieldDefn(fgd)
            feat = ogr.Feature(ffd)
            feat.SetGeometry(geom)
            lyr.CreateFeature(feat)

            # geom_list.append(geom)
            # print(len(geom_list))
            # create_shapefile(dst_shp, geom_list, utm_proj)
