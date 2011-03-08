import datetime, copy, uuid
import pymongo
from pymongo.connection import Connection as mongo_con
from pymongo import Connection

class FriskConnection(pymongo.Connection):
    """Extension to the PyMongo Connection class that adds a check_health method for verifying
    connectivity to the Master as well as ALL Slaves in the Replica Set
    """
    def check_health(self):
        """Returns the health of ALL nodes in a Replica Set in dictionary format.
        {'db_master_host': 'host1:27017', 'db_slave_hosts': ['host2:27018','host3:27019'], 
        'db_master_can_write': True, 'db_master_can_read': True,
        'db_slaves_can_read': [('host2',True),('host3',True)] }
        """
        db = 'monitoring'
        db_master_can_write = self._can_write_to_master(db)
        db_master_can_read = self._can_read_from_master(db)

        slaves = self._get_slave_hosts()
        db_slaves_can_read = self._can_read_from_slaves(slaves)

        return {'db_master_host': self.host + ':' + str(self.port),
                'db_slave_hosts': slaves,
                'db_master_can_read':db_master_can_read,
                'db_master_can_write':db_master_can_write,
                'db_slaves_can_read':db_slaves_can_read
                }

    def _get_slave_hosts(self):
        master_host = self.host
        master_port = self.port
        slaves = []
        for node in self.nodes:
            slave_host = node[0]
            slave_port = node[1]
            if '.' in slave_host:
                slave_host = str(slave_host).split(".",1)[0]
            if (slave_host != master_host or slave_port != master_port and not slave_host+':'+str(slave_port) in slaves):
                slaves.append(slave_host+':'+str(slave_port))
        return slaves
    
    def _can_read_from_slaves(self, slaves):
        db_slaves_can_read = []
        for slave in slaves:
            is_healthy = False
            slave_connection = None
            try:
                slave_connection = pymongo.connection.Connection(slave, network_timeout=2, slave_okay=True)
                stats=slave_connection.admin["$cmd.sys.inprog"].find_one()
                if(u"fsyncLock" not in stats.keys()):
                    if 'admin' in slave_connection.database_names():
                        is_healthy = True
                else:
                    is_healthy="Write Locked"
            except Exception as e:
                pass
            finally:
                if slave_connection:
                    slave_connection.disconnect()
            db_slaves_can_read.append((slave, is_healthy))
        return db_slaves_can_read

    def _can_write_to_master(self, db):
        master_db = self.db
        db_master_can_write = False
        try:
            id = self._get_new_uuid()
            test_data = {"_id":id, 'date': self._get_datetime_now_microseconds()}
            master_db['friskmonitoring'].save(test_data)
            db_master_can_write = master_db['friskmonitoring'].find_one({'_id':id}) == test_data
        except:
            pass
        finally:
            master_db['friskmonitoring'].remove({'_id':id})
        return db_master_can_write

    def _can_read_from_master(self, db):
        master_db = self.db
        db_master_can_read = False
        try:
            db_master_can_read = master_db.collection_names() != []
        except:
            pass
        return db_master_can_read

    def _get_datetime_now_microseconds(self):
        return datetime.datetime.now().microsecond
        
    def _get_new_uuid(self):
        return str(uuid.uuid1())

class PyMongoFrisk(object):
    """Wrapper for pymongo Connection allowing additional check_health method for checking connectivity 
    of master and slave in a Replica Pair running in Auth mode
    """
    def __init__(self, uri, **kw):
        self._uri = copy.copy(uri)
        self._parse_uri(uri)
        self._connection = mongo_con.from_uri(uri, **kw)

    @classmethod
    def from_uri(cls, uri, **kw):
        return cls(uri, **kw)

    def __getattr__(self, attr_name):
        return getattr(self._connection, attr_name)

    def __getitem__(self,name):
        return self._connection[name]

    def check_health(self):
        """Returns the health of a both nodes in a Replica Pair connection in dictionary format.
        {'db_master_url': 'host2', 'db_slave_url': 'host1', 
        'db_master_can_write': True, 'db_master_can_read': True, 
        'db_slave_can_read': True}
        """
        master_connection = self._connection[self._database]
        master = self._connection.host
        id = str(uuid.uuid1())
        test_data = {"_id":id, 'date': datetime.datetime.now().microsecond}

        db_master_can_write = False
        try:
            master_connection['friskmonitoring'].save(test_data)
            db_master_can_write = master_connection['friskmonitoring'].find_one({'_id':id}) == test_data
        except:
            pass
        finally:
            master_connection['friskmonitoring'].remove({'_id':id})

        db_master_can_read = False
        try:
            db_master_can_read = master_connection.collection_names() != []
        except:
            pass

        db_slave_can_read = False
        hosts = copy.copy(self._hosts)
        if len(hosts) > 1:
            hosts.remove(master)
            slave = hosts[0]
            slave_connection = None
            try:
                slave_uri = copy.copy(self._uri)
                if "," in slave_uri:
                    slave_uri = slave_uri.replace(master, '').replace(',','')
                    slave_uri = slave_uri.replace(master, '').replace(',','')
                slave_connection = mongo_con.from_uri(slave_uri,slave_okay=True)
                db_slave_can_read = slave_connection[self._database].collection_names() != []
            except:
                pass
            finally:
                if slave_connection:
                    slave_connection.disconnect()
        else:
            slave = None

        return {'db_master_url': master,
                'db_slave_url': slave,
                'db_master_can_read':db_master_can_read,
                'db_master_can_write':db_master_can_write,
                'db_slave_can_read':db_slave_can_read
                }

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

Connection = PyMongoFrisk
