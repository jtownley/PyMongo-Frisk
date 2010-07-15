import datetime, copy
import pymongo
from pymongo.connection import Connection

class PyMongoFrisk(object):
    def __init__(self):
        pass

    def from_uri(self,uri="mongodb://localhost", **connection_args):
        self._parse_uri(uri)
        self._connection = Connection.from_uri(uri, **connection_args)
        return self._connection

    def check_health(self):
        pass

    def _parse_uri(self, uri):
#        expected_pattern = "[A-Za-z0-9]://[A-Za-z0-9]:[A-Za-z0-9]@[.,A-Za-z0-9]/[A-Za-z0-9]"
#        re.match(expected_pattern, uri)
        rest  = uri.split("://")[1]
        if "@" in rest:
            auth, rest = rest.split("@")
            self._username , self._password = auth.split(":")
        else:
            self._username , self._password = None, None
        if "/" in rest:
            hosts, self._database = rest.split("/")
        else:
            self._database = None
            hosts = rest
        self._hosts = hosts.split(",")