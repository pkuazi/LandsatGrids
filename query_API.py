#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-24 08:53
#

"""

"""

import sys
import json
from urllib.parse import urlencode
from urllib3.connectionpool import HTTPConnectionPool

try:
    import cPickle as pickle
except:
    import pickle

url_prefix = "/databox"


class DataBoxConnector(object):
    def __init__(self, host, port, user="", passwd=""):
        self.c = HTTPConnectionPool(host, port)
        url = url_prefix + "/metadata/authenticate"
        headers = {
            "content-type": "application/x-www-form-urlencoded",
        }

        params = {
            "user": user,
            "passwd": passwd
        }
        resp = self.c.urlopen("POST", url, body=urlencode(params), headers=headers)

        jdata = json.loads(resp.data.decode("utf-8"))
        self.auth_code = jdata["auth_code"]

    def _load_resp(self, resp):
        ctype = resp.headers["Content-Type"]

        #         print( resp.headers )

        code = resp.headers.get("Error-Code", None)
        reason = resp.headers.get("Error-Message", None)

        if ctype.startswith("application/json") == True:
            jdata = json.loads(resp.data.decode("utf-8"))
        elif ctype.startswith("application/pickle-bytes") == True:
            jdata = pickle.loads(resp.data)
        else:
            jdata = None

        return jdata, resp.status, code, reason

    def _info_with(self, product, ctype, crs, params):
        url = url_prefix + "/info_with/{product}/{ctype}".format(product=product, ctype=ctype)

        params["crs"] = "" if crs is None else crs
        params["format"] = "bytes"

        headers = {
            "content-type": "application/x-www-form-urlencoded",
            "auth_code": self.auth_code
        }

        #         print(params)

        resp = self.c.urlopen("POST", url, body=urlencode(params), headers=headers)
        return self._load_resp(resp)

    def info_by_point(self, product, ctype, x, y, crs=None, timeslice=None):
        '''
        返回 x,y 坐标点所在 cube 信息，返回：bands, crs, bbox, res, size，xy, nctimes
        ctype：数据产品名称
        x, y：坐标
        crs：坐标对应投影信息，如为：None，默认：EPSG:4326
        '''
        return self._info_with(product, ctype, crs, {
            "x": x,
            "y": y,
            "times": "" if timeslice is None else timeslice
        })

    info_by_bbox(self, minx, miny, maxx, maxy, start_time, end_time, fmt="json")
    def info_by_bbox(self, bbox, start_time=None, end_time=None):
        return self._info_with(product, ctype, crs, {
            "bbox": ",".join(map(lambda a: str(a), bbox)),
            # "times": "" if timeslice is None else timeslice
            "start_time":start_time,
            "end_time":end_time
        })

    def info_by_geom(self, product, ctype, geom, crs=None, timeslice=None):
        return self._info_with(product, ctype, crs, {
            "geom": geom,
            "times": "" if timeslice is None else timeslice
        })

    def query_by_point(self, product, ctype, bandid, x, y, crs=None, timeslice=None):
        url = url_prefix + "/query_point/{product}/{ctype}".format(product=product, ctype=ctype)

        params = {"bandid": bandid, "x": x, "y": y}
        params["crs"] = "" if crs is None else crs
        params["times"] = "" if timeslice is None else timeslice
        params["format"] = "bytes"

        headers = {
            "content-type": "application/x-www-form-urlencoded",
            "auth_code": self.auth_code
        }

        #         print(params)

        resp = self.c.urlopen("POST", url, body=urlencode(params), headers=headers)
        return self._load_resp(resp)

    def query_by_geom(self, product, ctype, bandid, geom_info, timeslice=None):
        url = url_prefix + "/query_geom/{product}/{ctype}".format(product=product, ctype=ctype)

        params = {"bandid": bandid, }
        #         params[ "geom_info" ] = json.dumps(geom_info, ensure_ascii=False)
        params["mask_geom"] = geom_info["geometry"]
        params["grid_x"], params["grid_y"] = geom_info["xy"]
        params["times"] = "" if timeslice is None else timeslice
        params["format"] = "bytes"

        headers = {
            "content-type": "application/x-www-form-urlencoded",
            "auth_code": self.auth_code
        }

        #         print(params)

        resp = self.c.urlopen("POST", url, body=urlencode(params), headers=headers)
        return self._load_resp(resp)


if __name__ == '__main__':

    c = DataBoxConnector("www.gscloud.cn", 80)

    import time

    t = time.time()
    n = 1

    for _ in range(n):
        # timeslice = "year in [2008,2009,2010, 2011,2012]"

        query_polygon = {'type': 'Polygon', 'coordinates': [
            [[112.180, 22.367], [112.629, 22.383], [112.709, 21.965], [112.196, 21.9], [112.180, 22.367]]]}
        start_time = "20010117"
        end_time = "20150101"

        # info, status, code, reason = c.query_by_point("LANDSAT", "L45TM", 116.056832678626, 39.5991427612245, "EPSG:4326", timeslice=timeslice)
        # print(info, status, code, reason)

        info, status, code, reason = c.info_by_point("LANDSAT", "L45TM", 116.056832678626, 39.5991427612245,
                                                     "EPSG:4326", timeslice=timeslice)
        print(info, status, code, reason)

        info, status, code, reason = c.query_by_point("LANDSAT", "L45TM", "B10", 116.456832678626, 39.5991427612245,
                                                      "EPSG:4326", timeslice=timeslice)
        print(info, status, code, reason)

        info, status, code, reason = c.info_by_bbox("LANDSAT", "L45TM", [117.513, 40.013, 118.5243, 41.023, ],
                                                    "EPSG:4326", timeslice=timeslice)
        print(info, status, code, reason)

        gjson = """{ "type": "Polygon", "coordinates": [ [ [ 116.056832678625995, 39.599142761224542 ], [ 116.063241002197074, 39.599753077755125 ], [ 116.063241002197074, 39.599753077755125 ], [ 116.064156476992935, 39.594565387245204 ], [ 116.056222362095411, 39.590598329796435 ], [ 116.05225530464665, 39.589377696735284 ], [ 116.046762455871431, 39.593649912449337 ], [ 116.056832678625995, 39.599142761224542 ] ] ] }"""
        info, status, code, reason = c.info_by_geom("LANDSAT", "L45TM", gjson, "EPSG:4326", timeslice=timeslice)
        print(info, status, code, reason)

        if len(info) > 0:
            info, status, code, reason = c.query_by_geom("LANDSAT", "L45TM", "B40", info[0], timeslice=timeslice)
            print(info, status, code, reason)

    t = time.time() - t
    print(t / n)

