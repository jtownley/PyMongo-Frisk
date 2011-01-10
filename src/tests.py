import unittest, datetime
from pymongo_frisk import PyMongoFrisk as PMF, FriskConnection
from mock import patch, Mock
import pymongo.errors

class FriskConnectionStub(FriskConnection):
    __port = 27017
    __host = 'localhost'
    __nodes = set([('localhost', 27017), ('localhost', 27018), ('localhost', 27019)])
    
    def __init__(self, host=None, port=None, pool_size=None,
             auto_start_request=None, timeout=None, slave_okay=False,
             network_timeout=None, document_class=dict, tz_aware=False,
             _connect=True):
        self.database = DatabaseStub()
  
    @property
    def nodes(self):
        return self.__nodes
    
    @property
    def port(self):
        return self.__port
        
    @property
    def host(self):
        return self.__host
    
    def _get_datetime_now_microseconds(self):
        return 1
    
    def _get_new_uuid(self):
        return 1
        
class FriskConnectionTest(unittest.TestCase):

    host1 = 'localhost:27017'
    host2 = 'localhost:27018'
    host3 = 'localhost:27019'
    hosts = [host1,host2,host3] 
    health = {}
    connection = None
      
    def setUp(self):
        self.connection = FriskConnectionStub(self.hosts)
        
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')
    def test_check_health_shouldReturnMasterHost_whenConnectionCreated(self, mock_save, mock_remove):
        self.health =  self.connection.check_health()      
        self.assertEquals(self.host1, self.health['db_master_host'])      

    @patch.object(pymongo.collection.Collection, 'find_one')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')
    def test_check_health_shouldReportCanWriteToMaster_whenMasterIsUp(self, mock_save, mock_remove, mock_find_one):
        mock_find_one.return_value={"_id":1, 'date': 1}
        self.health =  self.connection.check_health()
        self.assertEquals(True, self.health['db_master_can_write'])

    @patch.object(pymongo.collection.Collection, 'find_one')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')
    def test_check_health_shouldReportCanWriteToMasterFailed_whenReadFromMasterDoesNotMatchDataWritten(self, mock_save, mock_remove, mock_find_one):
        mock_find_one.return_value={"_id":1, 'date': 1234}
        self.health =  self.connection.check_health()
        self.assertEquals(False, self.health['db_master_can_write'])

    @patch.object(pymongo.database.Database, 'collection_names')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')    
    def test_check_health_shouldReportCanReadFromMaster_whenMasterIsUp(self, mock_save, mock_remove, mock_database_collection_names):
        mock_database_collection_names.return_value=['collection1','collection2']
        self.health =  self.connection.check_health()
        self.assertEquals(True, self.health['db_master_can_read'])

    @patch.object(pymongo.database.Database, 'collection_names')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')    
    def test_check_health_shouldReportCanReadFromMasterFailed_whenReadFromMasterReturnsEmptyCollectionList(self, mock_save, mock_remove, mock_database_collection_names):
        mock_database_collection_names.return_value=[]
        self.health =  self.connection.check_health()
        self.assertEquals(False, self.health['db_master_can_read'])

    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')            
    def test_check_health_shouldReturnListOfSlaveHosts_whenMultipleNodesInReplicaSet(self, mock_save, mock_remove):
        self.health =  self.connection.check_health()
        self.assertTrue(self.host1 not in self.health['db_slave_hosts'])
        self.assertTrue(self.host2 in self.health['db_slave_hosts'])
        self.assertTrue(self.host3 in self.health['db_slave_hosts'])

    @patch('pymongo.connection.Connection')
    @patch.object(pymongo.collection.Collection, 'find_one')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')                
    def test_check_health_shouldReportCanReadFromAllSlaves_whenAllSlavesAreUp(self, mock_save, mock_remove, mock_find_one, mock_connection):
        mock_find_one.return_value={}
        mc = mock_connection.return_value
        mc.database_names.return_value = ['admin','local']
        self.health =  self.connection.check_health()
        self.assertTrue((self.host2,True) in self.health['db_slaves_can_read'])
        self.assertTrue((self.host3,True) in self.health['db_slaves_can_read'])
 
    @patch('pymongo.connection.Connection')
    @patch.object(pymongo.collection.Collection, 'find_one')
    @patch.object(pymongo.collection.Collection, 'remove')
    @patch.object(pymongo.collection.Collection, 'save')                
    def test_check_health_shouldReportCanReadFromAllSlavesFailed_whenFailToQuerySlaves(self, mock_save, mock_remove, mock_find_one, mock_connection):
        mock_find_one.return_value={}
        mc = mock_connection.return_value
        mc.database_names.return_value = []
        self.health =  self.connection.check_health()
        self.assertTrue((self.host2,False) in self.health['db_slaves_can_read'])
        self.assertTrue((self.host3,False) in self.health['db_slaves_can_read'])    
        
class PyMongoFriskTest(unittest.TestCase):

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_calls_parent_with_same(self, mock_from_uri):
        test_uri = "mongo://user:password@url.com/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF.from_uri(test_uri)
        mock_from_uri.assert_called_with(test_uri)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        mock_from_uri.return_value="Connection"
        expected_connection = PMF.from_uri(test_uri)
        self.assertEquals('username',expected_connection._username)
        self.assertEquals('password',expected_connection._password)
        self.assertEquals('database',expected_connection._database)
        self.assertEquals(['host1','host2'],expected_connection._hosts)
        self.assertEquals('Connection', expected_connection._connection)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_even_if_port_specified(self, mock_from_uri):
        test_uri = "mongo://username:password@host1:123,host2:456/database"
        mock_from_uri.return_value="Connection"
        expected_connection = PMF.from_uri(test_uri)
        self.assertEquals('username',expected_connection._username)
        self.assertEquals('password',expected_connection._password)
        self.assertEquals('database',expected_connection._database)
        self.assertEquals(['host1:123','host2:456'],expected_connection._hosts)
        self.assertEquals('Connection', expected_connection._connection)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_one_host(self, mock_from_uri):
        test_uri = "mongo://username:password@host1/database"
        mock_from_uri.return_value="Connection"
        expected_connection = PMF.from_uri(test_uri)
        self.assertEquals('username',expected_connection._username)
        self.assertEquals('password',expected_connection._password)
        self.assertEquals('database',expected_connection._database)
        self.assertEquals(['host1'],expected_connection._hosts)
        self.assertEquals('Connection', expected_connection._connection)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_no_db(self, mock_from_uri):
        test_uri = "mongo://username:password@host1"
        mock_from_uri.return_value="Connection"
        expected_connection = PMF.from_uri(test_uri)
        self.assertEquals('username',expected_connection._username)
        self.assertEquals('password',expected_connection._password)
        self.assertEquals(None,expected_connection._database)
        self.assertEquals(['host1'],expected_connection._hosts)
        self.assertEquals('Connection', expected_connection._connection)
#
    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_no_auth(self, mock_from_uri):
        test_uri = "mongo://host1"
        mock_from_uri.return_value="Connection"
        expected_connection = PMF.from_uri(test_uri)
        self.assertEquals(None,expected_connection._username)
        self.assertEquals(None,expected_connection._password)
        self.assertEquals(None,expected_connection._database)
        self.assertEquals(['host1'],expected_connection._hosts)
        self.assertEquals('Connection', expected_connection._connection)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_throws_exception_when_invalid_uri(self, mock_from_uri):
        test_uri = "mongo://username@host/database"
        mock_from_uri.return_value="Connection"
        self.assertRaises(pymongo.errors.InvalidURI, PMF.from_uri, test_uri)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_throws_exception_when_to_many_hosts(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2,host3/database"
        mock_from_uri.return_value="Connection"
        self.assertRaises(pymongo.errors.InvalidURI, PMF.from_uri, test_uri)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_and_slave_urls(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF.from_uri(test_uri)
        health = pmf.check_health()
        self.assertEquals('host1',health['db_master_url'])
        self.assertEquals('host2',health['db_slave_url'])

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_read_and_write_success(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF.from_uri(test_uri)
        health = pmf.check_health()
        self.assertTrue(health['db_master_can_read'])
        self.assertTrue(health['db_master_can_write'])
        self.assertTrue(connection_stub.database.collection.remove_called)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_read_fail_when_master_cannot_be_read(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        connection_stub.database.collections = []
        mock_from_uri.return_value=connection_stub
        pmf = PMF.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_master_can_read'])

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_write_fail_when_master_cannot_write(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        connection_stub.database.collection.override_data = True
        connection_stub.database.collection.data = None
        mock_from_uri.return_value=connection_stub
        pmf = PMF.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_master_can_write'])

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_slave_read_success(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        expected_slave_uri = "mongo://username:password@host2/database"
        master_connection_stub = ConnectionStub()
        slave_connection_stub = ConnectionStub()
        slave_connection_stub.host = 'host2'
        mock_from_uri.return_value=master_connection_stub
        pmf = PMF.from_uri(test_uri)
        mock_from_uri.return_value=slave_connection_stub
        health = pmf.check_health()

        mock_from_uri.assert_called_with(expected_slave_uri, slave_okay=True)
        self.assertTrue(health['db_slave_can_read'])
        self.assertTrue(slave_connection_stub.disconnect_called)

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_handles_no_slave(self, mock_from_uri):
        test_uri = "mongo://username:password@host1/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_slave_can_read'])
        self.assertEquals(None, health['db_slave_url'])

    @patch.object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_slave_read_fail_when_slave_cannot_read(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        expected_slave_uri = "mongo://username:password@host2/database"
        master_connection_stub = ConnectionStub()
        slave_connection_stub = ConnectionStub()
        slave_connection_stub.database.collections = []
        slave_connection_stub.host = 'host2'
        mock_from_uri.return_value=master_connection_stub
        pmf = PMF.from_uri(test_uri)
        mock_from_uri.return_value=slave_connection_stub
        health = pmf.check_health()

        mock_from_uri.assert_called_with(expected_slave_uri, slave_okay=True)
        self.assertFalse(health['db_slave_can_read'])
        self.assertTrue(slave_connection_stub.disconnect_called)

class ConnectionStub(object):
    def __init__(self):
        self.defaults()

    def defaults(self):
        self.host= "host1"
        self.database = DatabaseStub()
        self.disconnect_called = False

    def __getitem__(self,name):
        return self.database

    def disconnect(self):
        self.disconnect_called = True

class DatabaseStub(object):
    def __init__(self):
        self.defaults()

    def defaults(self):
        self.collection = CollectionStub()
        self.drop_collection_called = False
        self.disconnect_called = False
        self.collections = ['friskmonitoring']

    def collection_names(self):
        return self.collections

    def __getitem__(self,name):
        return self.collection

    def drop_collection(self, collection_name):
        self.drop_collection_called = True

class CollectionStub(object):
    def __init__(self):
        self.defaults()

    def defaults(self):
        self.override_data = False
        self.remove_called = False

    def save(self, data):
        if not self.override_data:
            self.data = data

    def find_one(self, query):
        return self.data

    def remove(self, query):
        self.remove_called = True

if __name__=='__main__':
    unittest.main()


    