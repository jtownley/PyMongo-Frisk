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
        try:
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
            if len(self._hosts) > 2:
                raise pymongo.errors.InvalidURI("Too many hosts")
        except:
            raise pymongo.errors.InvalidURI("Uri not in expected format")



#    def test_connections(self):
#        master = self._connection.connection.host
#        hosts = copy.copy(self._hosts)
#        hosts.remove(master)
#        slave = hosts[0]
#        test_data = {"_id":self._database, 'date': datetime.datetime.now().microsecond}
#
#        db_master_read = False
#        try:
#            db_master_read = self._connection.collection_names() != []
#        except:
#            pass
#
#        db_master_write = False
#        try:
#            self._connection['friskmonitoring'].save(test_data)
#            db_master_write = self._connection['friskmonitoring'].find_one({'_id':self._database}) == test_data
#        except:
#            pass
#        finally:
#            self._connection.drop_collection('friskmonitoring')
#
#        db_slave_read = False
#        try:
#            slave_connection = self._create_connection([slave])
#            db_slave_read = self._connection.collection_names() != []
#        except:
#            pass
#        finally:
#            slave_connection.connection.disconnect()
#
#        return {'db_master_url': master, 'db_slave_url': slave, 'db_master_read':db_master_read,
#                'db_slave_read': db_slave_read, 'db_master_write': db_master_write}
#
#    def _create_connection(self, hosts):
#        host_list = ",".join(self._hosts)
#        return pymongo.Connection.from_uri(
#                "mongodb://%s:%s@%s%s" % (self._username, self._password, host_list, "/" + self._database))[self._database]



