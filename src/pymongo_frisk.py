from pymongo.connection import Connection

class PyMongoFrisk(Connection):

    @classmethod
    def from_uri(cls, uri="mongodb://localhost", **connection_args):
        return super(PyMongoFrisk,cls).from_uri(uri, **connection_args)