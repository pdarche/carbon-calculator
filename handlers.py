#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.web
import tornado.gen
import pymongo

import mixins
from config import settings


client = pymongo.MongoClient('localhost', 27017)
db = client.carbon_calculator

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
            user = yield self.get_authenticated_user(
                redirect_uri='http://127.0.0.1:8000/moves',
                code=self.get_argument("code"))
            db.users.insert(user)
        else:
            yield self.authorize_redirect(
                redirect_uri='http://127.0.0.1:8000/moves',
                client_id=settings["moves_client_id"],
                scope=['activity', 'location'],
                response_type="code")

