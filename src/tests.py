import unittest
import pymongo
from pymongo_frisk import PyMongoFrisk as PMF
from mock import patch_object, patch, Mock
import pymongo.errors

class PyMongoFriskTest(unittest.TestCase):


    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_calls_parent_with_same(self, mock_from_uri):
        test_uri = "mongo://user:password@url.com/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        pmf.from_uri(test_uri)
        mock_from_uri.assert_called_with(test_uri)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        expected_connection = pmf.from_uri(test_uri)
        self.assertEquals('username',pmf._username)
        self.assertEquals('password',pmf._password)
        self.assertEquals('database',pmf._database)
        self.assertEquals(['host1','host2'],pmf._hosts)
        self.assertEquals('Connection', expected_connection)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_even_if_port_specified(self, mock_from_uri):
        test_uri = "mongo://username:password@host1:123,host2:456/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        expected_connection = pmf.from_uri(test_uri)
        self.assertEquals('username',pmf._username)
        self.assertEquals('password',pmf._password)
        self.assertEquals('database',pmf._database)
        self.assertEquals(['host1:123','host2:456'],pmf._hosts)
        self.assertEquals('Connection', expected_connection)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_one_host(self, mock_from_uri):
        test_uri = "mongo://username:password@host1/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        expected_connection = pmf.from_uri(test_uri)
        self.assertEquals('username',pmf._username)
        self.assertEquals('password',pmf._password)
        self.assertEquals('database',pmf._database)
        self.assertEquals(['host1'],pmf._hosts)
        self.assertEquals('Connection', expected_connection)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_no_db(self, mock_from_uri):
        test_uri = "mongo://username:password@host1"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        expected_connection = pmf.from_uri(test_uri)
        self.assertEquals('username',pmf._username)
        self.assertEquals('password',pmf._password)
        self.assertEquals(None,pmf._database)
        self.assertEquals(['host1'],pmf._hosts)
        self.assertEquals('Connection', expected_connection)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_creates_expected_details_given_no_auth(self, mock_from_uri):
        test_uri = "mongo://host1"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        expected_connection = pmf.from_uri(test_uri)
        self.assertEquals(None,pmf._username)
        self.assertEquals(None,pmf._password)
        self.assertEquals(None,pmf._database)
        self.assertEquals(['host1'],pmf._hosts)
        self.assertEquals('Connection', expected_connection)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_throws_exception_when_invalid_uri(self, mock_from_uri):
        test_uri = "mongo://username@host/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        self.assertRaises(pymongo.errors.InvalidURI, pmf.from_uri, test_uri)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_throws_exception_when_to_many_hosts(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2,host3/database"
        mock_from_uri.return_value="Connection"
        pmf = PMF()
        self.assertRaises(pymongo.errors.InvalidURI, pmf.from_uri, test_uri)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_and_slave_urls(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        health = pmf.check_health()
        self.assertEquals('host1',health['db_master_url'])
        self.assertEquals('host2',health['db_slave_url'])

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_read_and_write_success(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        health = pmf.check_health()
        self.assertTrue(health['db_master_can_read'])
        self.assertTrue(health['db_master_can_write'])
        self.assertTrue(connection_stub.database.drop_collection_called)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_read_fail_when_master_cannot_be_read(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        connection_stub.database.collections = []
        mock_from_uri.return_value=connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_master_can_read'])

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_master_write_fail_when_master_cannot_write(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        connection_stub = ConnectionStub()
        connection_stub.database.collection.override_data = True
        connection_stub.database.collection.data = None
        mock_from_uri.return_value=connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_master_can_write'])

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_slave_read_success(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        expected_slave_uri = "mongo://username:password@host2/database"
        master_connection_stub = ConnectionStub()
        slave_connection_stub = ConnectionStub()
        slave_connection_stub.host = 'host2'
        mock_from_uri.return_value=master_connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        mock_from_uri.return_value=slave_connection_stub
        health = pmf.check_health()

        mock_from_uri.assert_called_with(expected_slave_uri)
        self.assertTrue(health['db_slave_can_read'])
        self.assertTrue(slave_connection_stub.disconnect_called)

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_handles_no_slave(self, mock_from_uri):
        test_uri = "mongo://username:password@host1/database"
        connection_stub = ConnectionStub()
        mock_from_uri.return_value=connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        health = pmf.check_health()
        self.assertFalse(health['db_slave_can_read'])
        self.assertEquals(None, health['db_slave_url'])

    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_check_health_returns_slave_read_fail_when_slave_cannot_read(self, mock_from_uri):
        test_uri = "mongo://username:password@host1,host2/database"
        expected_slave_uri = "mongo://username:password@host2/database"
        master_connection_stub = ConnectionStub()
        slave_connection_stub = ConnectionStub()
        slave_connection_stub.database.collections = []
        slave_connection_stub.host = 'host2'
        mock_from_uri.return_value=master_connection_stub
        pmf = PMF()
        pmf.from_uri(test_uri)
        mock_from_uri.return_value=slave_connection_stub
        health = pmf.check_health()

        mock_from_uri.assert_called_with(expected_slave_uri)
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
        self.collections = ['Col1','Col2']

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

    def save(self, data):
        if not self.override_data:
            self.data = data

    def find_one(self, query):
        return self.data

if __name__=='__main__':
    unittest.main()


    