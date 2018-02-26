#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-24 08:52
#

"""

"""
# ! /usr/bin/python3

'''
Created on Sep 25, 2017

@author: root
'''

from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado import web
from tornado import gen
from tornado import concurrent

from LandsatGrids import QueryProvider
# from databox import EDatabox

import sys
import json
from collections import namedtuple

import threading
import queue

FutureTask = namedtuple('FutureTask', ['future', 'fn', 'args', 'kwargs'])

es_host = "10.0.138.156"
es_port = 9200

class FuturesExecutor(object):
    _instance_lock = threading.Lock()

    _current = threading.local()

    @staticmethod
    def instance(pool_size=4):
        if not hasattr(FuturesExecutor, "_instance"):
            with FuturesExecutor._instance_lock:
                if not hasattr(FuturesExecutor, "_instance"):
                    FuturesExecutor._instance = FuturesExecutor(pool_size)
        return FuturesExecutor._instance

    @staticmethod
    def current(instance=True, pool_size=4):
        current = getattr(FuturesExecutor._current, "instance", None)
        if current is None and instance:
            return FuturesExecutor.instance(pool_size)
        return current

    def __init__(self, pool_size=4):
        FuturesExecutor._current.instance = self

        self.terminated = False
        self.task_queue = queue.Queue()
        self.threads = []
        for idx in range(pool_size):
            t = threading.Thread(name="executor_%s" % (idx,), target=self.run_loop, daemon=True)
            self.threads.append(t)
            t.start()

    def run_once(self, func, *args, **kwargs):
        try:
            cinfo, cfmt = func(*args, **kwargs)
            return cinfo, cfmt, 0, "Success"
        except EDatabox as e:
            return None, None, e.code, e.reason

    def run_loop(self):
        while self.terminated == False:
            task = self.task_queue.get()
            try:
                result = self.run_once(task.fn, *task.args, **task.kwargs)
                task.future.set_result(result)
            except Exception:
                task.future.set_exc_info(sys.exc_info())
            self.task_queue.task_done()

    def submit(self, fn, *args, **kwargs):
        future = concurrent.TracebackFuture()
        task = FutureTask(future=future, fn=fn, args=args, kwargs=kwargs)
        self.task_queue.put(task)
        return future

    def shutdown(self, wait=True):
        if wait == True:
            self.task_queue.join()
        else:
            self.terminated = True
            for t in self.threads:
                t.join()


class BaseHandler(web.RequestHandler):
    def initialize(self, provider=None, pool_size=4):
        self.provider = provider
        self.executor = FuturesExecutor.current(pool_size=pool_size)
        self.io_loop = IOLoop.current()

    @concurrent.run_on_executor(executor="executor", io_loop="io_loop")
    def async_dojob(self, func, *args, **kwargs):
        return func(*args)

    def after_dojob(self, resp):
        cinfo, cfmt, code, reason = resp

        self.add_header("Error-Code", code)
        self.add_header("Error-Message", reason)

        if code == 0:
            if cfmt == "json":
                self.set_header("Content-Type", "application/json; charset=UTF-8")
                self.write(json.dumps(cinfo, ensure_ascii=False))
            else:
                self.set_header("Content-Type", "application/pickle-bytes; charset=UTF-8")
                self.write(cinfo)
        self.finish()


class QueryPointHandler(BaseHandler):
    #     @gen.coroutine
    @web.asynchronous
    def post(self, product, ctype):
        query = self.provider.get_query(product)

        tif_file = self.get_argument("tif_file")

        pt = self.get_argument("pt", None)
        if pt:
            pt = list(map(lambda a: float(a), pt.split(",")))
            x, y = pt
        else:
            x = float(self.get_argument("x"))
            y = float(self.get_argument("y"))

        self.async_dojob(query.read_by_point, tif_file, x, y, callback=self.after_dojob)

    #         cinfo, cfmt = query.query_by_point(ctype , bandid, x, y, crs, timeslice, fmt)
    #         data = query.read_by_point(self, x, y, tif_file)
    #         after_dojob( ( cinfo, cfmt ) )

    get = post


class QueryGeomHandler(BaseHandler):
    #     @gen.coroutine
    @web.asynchronous
    def post(self, product, ctype):
        query = self.provider.get_query(product)

        wgs_geometry = self.get_argument("wgs_geometry")
        # fmt = self.get_argument("format", "json")

        tif_file = self.get_argument("tif_file")

        # mask_geom = self.get_argument("mask_geom", None)
        # grid_x = self.get_argument("grid_x", None)
        # grid_y = self.get_argument("grid_y", None)

        if wgs_geometry is None or tif_file is None:
            raise web.HTTPError(400)

        self.async_dojob(query.read_by_geom, ctype, wgs_geometry, tif_file, callback=self.after_dojob)

    #         cinfo, cfmt = query.query_by_geom(ctype, bandid , mask_geom, int(grid_x), int(grid_y) ,  timeslice, fmt)
    #         after_dojob( ( cinfo, cfmt ) )

    get = post


class InfoWithHandler(BaseHandler):
    #     @gen.coroutine
    @web.asynchronous
    def post(self, product, ctype):
        query = self.provider.get_query(es_host, es_port)
        fmt = self.get_argument("format", "json")

        start_time = self.get_argument("start_time")
        end_time = self.get_argument("end_time")
        # "format": "yyyyMMdd||yyyy"

        # crs = self.get_argument("crs", None)
        bbox = self.get_argument("bbox", None)
        if bbox:
            '''
    (prefix + "/+info_query/+(\w+)/+(\w+)/?", InfoWithHandler, options),
    获取 bbox 的数据信息
    http://127.0.0.1:8888/databox/info_query/LANDSAT/L45TM?bbox=117.513,40.013,118.5243,41.023&crs=
    '''
            bbox = list(map(lambda a: float(a), bbox.split(",")))
            minx, miny, maxx, maxy = bbox
            if minx > maxx or miny > maxy:
                raise web.HTTPError(400)

            self.async_dojob(query.query_by_bbox, ctype, minx, miny, maxx, maxy, start_time, end_time, fmt,
                             callback=self.after_dojob)

            #             cinfo, cfmt = query.info_by_bbox(self, minx, miny, maxx, maxy, start_time, end_time, fmt="json"):
            #             after_dojob( ( cinfo, cfmt ) )
            return

        geom = self.get_argument("geom", None)
        if geom:
            '''
    获取 geom 的数据信息
    http://127.0.0.1:8888/databox/info_query/LANDSAT/L45TM?geom=&crs=
    '''
            self.async_dojob(query.query_by_geom, geom, start_time, end_time, fmt, callback=self.after_dojob)

            #             cinfo = info_by_geom(self, geom, start_time, end_time, fmt="json"):
            #             after_dojob( ( cinfo, cfmt ) )

            return

        '''
    获取 point 的数据信息
    http://127.0.0.1:8888/databox/info_query/LANDSAT/L45TM?x=116.056832678626&y=39.5991427612245&crs=
    '''
        pt = self.get_argument("pt", None)
        if pt:
            pt = list(map(lambda a: float(a), pt.split(",")))
            x, y = pt
        else:
            x = float(self.get_argument("x"))
            y = float(self.get_argument("y"))

        self.async_dojob(query.info_by_point,  x, y, start_time, end_time, fmt, callback=self.after_dojob)

    #         cinfo, cfmt = query.query_by_point(self, x, y, start_time, end_time, fmt="json"):
    #         after_dojob( ( cinfo, cfmt ) )

    get = post


class MetaDataHandler(BaseHandler):
    def do1(self, keywd0):
        if keywd0 == "products":
            cinfo = self.provider.products()
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(json.dumps(cinfo, ensure_ascii=False))
            return

        if keywd0 == "authenticate":
            cinfo = {"auth_code": "ok"}
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(json.dumps(cinfo, ensure_ascii=False))
            return

    def do2(self, keywd0, keywd1):
        if keywd0 == "sensors":
            cinfo = self.provider.sensors(keywd1)
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(json.dumps(cinfo, ensure_ascii=False))
            return

    def do3(self, keywd0, keywd1, keywd2):
        pass

    @gen.coroutine
    def post(self, *args):
        if len(args) == 1:
            return self.do1(args[0])

        if len(args) == 2:
            return self.do2(args[0], args[1])

        if len(args) == 3:
            return self.do2(args[0], args[1], args[2])

    get = post


def make_app(pool_size=4):
    options = {
        "provider": QueryProvider(),
        "pool_size": pool_size,
    }
    # the application object include a routing table that maps requests (a regular expression) to handlers
    prefix = "/+databox"
    return web.Application([
        (prefix + "/+info_query/+(\w+)/+(\w+)/?", InfoWithHandler, options),
        (prefix + "/+read_point/+(\w+)/+(\w+)/?", QueryPointHandler, options),
        (prefix + "/+read_geom/+(\w+)/+(\w+)/?", QueryGeomHandler, options),

        (prefix + "/+metadata/+(\w+)/?", MetaDataHandler, options),
        (prefix + "/+metadata/+(\w+)/+(\w+)/?", MetaDataHandler, options),
        (prefix + "/+metadata/+(\w+)/+(\w+)/+(\w+)/?", MetaDataHandler, options),
    ],
        gzip=True
    )


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument('-pool', action='store', type=int, default=4, dest='pool_size',
                        help=u'number of executors, the default is 4')
    parser.add_argument('-fork', action='store', type=int, default=0, dest='fork_size',
                        help=u'number of forks, the default is 0')
    parser.add_argument('-port', action='store', type=int, default=8888, dest='serv_port', help=u'server port')

    options = parser.parse_args()

    app = make_app(options.pool_size)

    print("LandsatGridsServer started at port:", options.serv_port)

    if options.fork_size < 0: options.fork_size = 0
    if options.fork_size > 32: options.fork_size = 32

    if options.fork_size == 1:
        app.listen(options.serv_port, "0.0.0.0")
    else:
        server = HTTPServer(app)
        server.bind(options.serv_port)
        server.start(options.fork_size)

    IOLoop.current().start()

