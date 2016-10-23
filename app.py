#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import tornado.auth
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.autoreload
from tornado.options import options
import tornado.web

import handlers
from config import settings

if __name__ == "__main__":
    settings = dict(
        cookie_secret=settings['cookie_secret'],
        template_path=os.path.join(os.path.dirname(__file__), 'templates'),
        static_path=os.path.join(os.path.dirname(__file__), 'static')
    )
    app = tornado.web.Application(
        debug=True,
        handlers = [
            (r"/", handlers.MainHandler),
            (r"/moves", handlers.MovesConnectHandler),
            (r"/transports", handlers.TransportsHandler),
            (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": "./static"}),
        ],
        **settings
    )
    tornado.autoreload.start()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(8000)
    tornado.ioloop.IOLoop.instance().start()
