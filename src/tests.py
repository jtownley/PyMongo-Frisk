import unittest

from pymongo_frisk import PyMongoFrisk as PMF
from mock import patch_object
from pymongo.connection import Connection


class PyMongoFriskTest(unittest.TestCase):

    @patch_object(Connection,'from_uri')
    def test_connection_from_uri_calls_parent_with_same(self, mock_from_uri):
        #Setup
        test_uri = "mongo://user:password@url.com/database"
        mock_from_uri.return_value="foo"

        #Execute
        PMF.from_uri(test_uri)

        #Assert
        mock_from_uri.assert_called_with(test_uri)

if __name__=='__main__':
    unittest.main()


    