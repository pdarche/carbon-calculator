#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools

from tornado import escape
from tornado import httpclient
from tornado.auth import OAuthMixin, OAuth2Mixin
from tornado.auth import _auth_return_future
from config import settings

try:
    import urlparse  # py2
except ImportError:
    import urllib.parse as urlparse  # py3

try:
    import urllib.parse as urllib_parse  # py3
except ImportError:
    import urllib as urllib_parse  # py2


class MovesMixin(OAuth2Mixin):
    """Moves authentication using OAuth2."""
    _OAUTH_ACCESS_TOKEN_URL = "https://api.moves-app.com/oauth/v1/access_token"
    _OAUTH_AUTHORIZE_URL    = "https://api.moves-app.com/oauth/v1/authorize"
    _OAUTH_AUTHENTICATE_URL = "https://api.moves-app.com/oauth/v1/authenticate"

    _BASE_URL = "https://api.moves-app.com/api/1.1"

    @_auth_return_future
    def get_authenticated_user(self, redirect_uri, code, callback):
        """Handles the login for the Moves user, returning a user object."""
        http = self.get_auth_http_client()
        body = urllib_parse.urlencode({
            "redirect_uri": redirect_uri,
            "code": code,
            "client_id": settings['moves_client_id'], #self.settings[self._OAUTH_SETTINGS_KEY]['key'],
            "client_secret": settings['moves_client_secret'],#self.settings[self._OAUTH_SETTINGS_KEY]['secret'],
            "grant_type": "authorization_code",
        })

        http.fetch(self._OAUTH_ACCESS_TOKEN_URL,
                   functools.partial(self._on_access_token, callback),
                   method="POST", headers={'Content-Type': 'application/x-www-form-urlencoded'}, body=body)

    def _on_access_token(self, future, response):
        """Callback function for the exchange to the access token."""
        if response.error:
            future.set_exception(AuthError('Moves auth error: %s' % str(response)))
            return

        args = escape.json_decode(response.body)
        future.set_result(args)

    def get_auth_http_client(self):
        """Returns the `.AsyncHTTPClient` instance to be used for auth requests.

        May be overridden by subclasses to use an HTTP client other than
        the default.
        """
        return httpclient.AsyncHTTPClient()

