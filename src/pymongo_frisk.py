import datetime, copy
import pymongo
from pymongo.connection import Connection

class PyMongoFrisk(object):
    def __init__(self):
        pass

    def from_uri(self,uri="mongodb://localhost", **connection_args):
        self._uri = copy.copy(uri)
        self._parse_uri(uri)
        self._connection = Connection.from_uri(uri, **connection_args)
        return self._connection

    def check_health(self):
        test_connection = self._connection[self._database]
        master = self._connection.host

        test_data = {"_id":self._database, 'date': datetime.datetime.now().microsecond}

        db_master_can_read = False
        try:
            db_master_can_read = test_connection.collection_names() != []
        except:
            pass

        db_master_can_write = False
        try:
            test_connection['friskmonitoring'].save(test_data)
            db_master_can_write = test_connection['friskmonitoring'].find_one({'_id':self._database}) == test_data
        except:
            pass
        finally:
            test_connection.drop_collection('friskmonitoring')

        db_slave_can_read = False
        hosts = copy.copy(self._hosts)
        if len(hosts) > 1:
            hosts.remove(master)
            slave = hosts[0]
            try:
                slave_uri = copy.copy(self._uri)
                if "," in slave_uri:
                    slave_uri = slave_uri.replace(master, '').replace(',','')
                    slave_uri = slave_uri.replace(master, '').replace(',','')
                slave_connection = Connection.from_uri(slave_uri)
                db_slave_can_read = slave_connection[self._database].collection_names() != []
            except:
                pass
            finally:
                slave_connection.disconnect()
        else:
            slave = None

        return {'db_master_url': master,
                'db_slave_url': slave,
                'db_master_can_read':db_master_can_read,
                'db_master_can_write':db_master_can_write,
                'db_slave_can_read':db_slave_can_read}

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
