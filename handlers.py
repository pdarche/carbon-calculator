#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from bson import json_util
import dateutil.parser
from datetime import timedelta
import pymongo
import tornado.web
import tornado.gen
import datetime

import mixins
from config import settings


client = pymongo.MongoClient('localhost', 27017)
db = client.carbon

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("user")


class MainHandler(BaseHandler):
    def get(self):
        self.render('index.html')


class MovesConnectHandler(tornado.web.RequestHandler,
                        mixins.MovesMixin):
    @tornado.gen.coroutine
    def get(self):
        if self.get_argument('code', False):
            profile = yield self.get_authenticated_user(
                redirect_uri='http://127.0.0.1:8000/moves',
                code=self.get_argument("code"))
            # this should be some sort of find or create
            db.users.insert(profile)
        else:
            yield self.authorize_redirect(
                redirect_uri='http://127.0.0.1:8000/moves',
                client_id=settings["moves_client_id"],
                scope=['activity', 'location'],
                response_type="code")


class TransportsHandler(tornado.web.RequestHandler):
    def get(self):
        date = self.get_argument('date', datetime.datetime(2014, 1, 1, 0, 0))
        types = self.get_argument('types', 'airplane,car,bus,subway')
        transports = list(db.moves2.find(
            {'startDatetime': {'$gte': date}, 'type': {'$in': types.split(',')}},
            {'_id': 0, "startDatetime": 1, "startTime": 1, "type": 1, "carbon": 1}
        ).sort('startDatetime', 1))
        self.write(json.dumps({"data": transports}, default=json_util.default))


