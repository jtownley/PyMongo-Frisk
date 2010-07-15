import unittest
import pymongo
from pymongo_frisk import PyMongoFrisk as PMF
from mock import patch_object, patch
import pymongo.errors

class PyMongoFriskTest(unittest.TestCase):


    @patch_object(pymongo.connection.Connection, 'from_uri')
    def test_connection_from_uri_calls_parent_with_same(self, mock_from_uri):
        test_uri = "mongo://user:password@url.com/database"
        mock_from_uri.return_value="foo"
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

if __name__=='__main__':
    unittest.main()


    