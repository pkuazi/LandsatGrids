#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-08 11:06
#

"""
generate grids in wgs84 and utm with same gridid--zone_id
"""
import os
from geomtrans import GeomTrans
import fiona
import ogr, osr
import numpy as np
import json

if __name__ == '__main__':
    data_path = '/mnt/win/L45grids'
    grid_50 = os.path.join(data_path, 'grid_50.shp')

    vector = fiona.open(grid_50, 'r')
    json_list = []
    for feature in vector:
        json_list.append(feature)
    in_proj = vector.crs_wkt

    # json_list = []
    # dr = ogr.GetDriverByName("ESRI Shapefile")
    # shp_ds = dr.Open(grid_50)
    # layer = shp_ds.GetLayer(0)
    # sr = layer.GetSpatialRef()
    # in_proj = sr.ExportToWkt()
    # feat_num = layer.GetFeatureCount()
    # for i in range(feat_num):
    #     feat = layer.GetFeature(i)
    #     geom = feat.GetGeometryRef()
    #
    #     geojson = geom.ExportToWkt()
    #     json_list.append(geojson)

    wgs_proj = 'EPSG:4326'
    utm2wgs = GeomTrans(in_proj, wgs_proj)

    china_zone = range(40, 61)
    for zone in china_zone:
        utm_proj = "EPSG:326%s" % (zone)
        print(utm_proj)
        wgs2utm = GeomTrans(wgs_proj, utm_proj)

        # move geom by mv_degree
        mv_degree = (zone - 50) * 6

        # create two shps: wgs and utm
        # 1 get driver
        dr = ogr.GetDriverByName("ESRI Shapefile")

        # 2 create shapedata
        wgs_shp = os.path.join(data_path, 'wgs_grid_%s.shp' % zone)
        utm_shp = os.path.join(data_path, 'utm_grid_%s.shp' %zone)
        if os.path.exists(wgs_shp):
            os.remove(wgs_shp)
        if os.path.exists(utm_shp):
            os.remove(utm_shp)

        wgs_ds = dr.CreateDataSource(wgs_shp)
        utm_ds = dr.CreateDataSource(utm_shp)

        # 3 create spatial reference
        wgs_sr = osr.SpatialReference()
        wgs_sr.SetFromUserInput(wgs_proj)

        utm_sr = osr.SpatialReference()
        utm_sr.SetFromUserInput(utm_proj)

        # 4 create layer
        wgs_lyrName = os.path.splitext(os.path.split(wgs_shp)[1])[0]
        wgs_lyr = wgs_ds.CreateLayer(wgs_lyrName, wgs_sr, ogr.wkbPolygon)
        wgs_layerDefn = wgs_lyr.GetLayerDefn()

        utm_lyrName = os.path.splitext(os.path.split(utm_shp)[1])[0]
        utm_lyr = utm_ds.CreateLayer(utm_lyrName, utm_sr, ogr.wkbPolygon)
        utm_layerDefn = utm_lyr.GetLayerDefn()

        # 5 create a field
        idField = ogr.FieldDefn('GridID', ogr.OFTString)
        # geomField = ogr.GeomFieldDefn('the_geom', ogr.wkbPolygon)
        wgs_lyr.CreateField(idField)
        # wgs_lyr.CreateGeomField(geomField)
        # wgs_lyr.CreateField(geomField)
        utm_lyr.CreateField(idField)

        # for each grid cell
        id = 0
        for geo_json in json_list:
            corner_points = geo_json['geometry']['coordinates'][0]
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

            wgs_geojson_moved = {'coordinates': [corner_points_moved], 'type': 'Polygon'}
            wgs_geojson_moved = json.dumps(wgs_geojson_moved)

            # wgs to utm
            utm_geojson_moved = {'coordinates': [wgs2utm.transform_points(corner_points_moved)], 'type': 'Polygon'}
            utm_geojson_moved = json.dumps(utm_geojson_moved)

            # 6 create polygon geometry
            wgs_geom = ogr.CreateGeometryFromJson(wgs_geojson_moved)
            wgs_geom_polygon = ogr.ForceToPolygon(wgs_geom)
            utm_geom = ogr.CreateGeometryFromJson(utm_geojson_moved)

            # 7 create feature
            wgs_feat = ogr.Feature(wgs_layerDefn)
            wgs_feat.SetGeometry(wgs_geom)
            wgs_feat.SetField('GridID', '%s_%s'%(zone,id))
            # wgs_feat.SetGeomField('the_geom', wgs_geom)

            utm_feat = ogr.Feature(utm_layerDefn)
            utm_feat.SetGeometry(utm_geom)
            utm_feat.SetField('GridID', '%s_%s' % (zone, id))

            # 8 save feature
            wgs_lyr.CreateFeature(wgs_feat)
            utm_lyr.CreateFeature(utm_feat)
            id = id+1

        # close datasource
        wgs_ds.Destroy()
        utm_ds.Destroy()
