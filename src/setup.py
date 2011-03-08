#!/usr/bin/env python
import os
from setuptools import setup

"""Copyright 2010 - 2011 - James Townley & Brian Richardson
Release under Apache-BSD style license

Source: http://github.com/jtownley/PyMongo-Frisk


Version: 0.0.6

----------------------------------
Description
----------------------------------

Wrapper for PyMongo Connection that offers additional application level
monitoring.
When connecting to multiple MongoDB servers, allows you to verify connectivity
to MongoDB slave servers.

----------------------------------
Problem this solves
----------------------------------

Although the built in monitoring page for mongo is awesome it misses one
situation.
This is when the mongo servers can talk to each other and the app can talk to
the master but the application has not been verified to be able to communicate
with the slave.

         _______________
        |               |
        |  APPLICATION  |
        |_______________|
           /         \
          /          (X)
  _______/___       ___\_______
 |           |     |           |
 |  Master   |-----|   Slave   |
 |___________|     |___________|


----------------------------------
Usage
----------------------------------
Use it as you would pymongo and call the check health method on connection

--Replica Sets--

    from pymongo_frisk import FriskConnection as Connection
    connection = Connection(['host1','host2','host3'])
    results = connection.check_health()

results is a dictionary as follows
{'db_master_host': 'host1:27017', 'db_slave_hosts': ['host2:27018','host3:27019'], 
    'db_master_can_write': True, 'db_master_can_read': True,
    'db_slaves_can_read': [('host2',True),('host3',True)] }
        
--Replica Pairs with Auth--

    import pymongo_frisk as pymongo
    connection = pymongo.connection.Connection.from_uri("mongo://username:password@host1,host2/database")
    results = connection.check_health()

results is a dictionary as follows:
{'db_master_url': 'host2', 'db_slave_url': 'host1', 'db_master_can_write': True,
    'db_slave_can_read': True, 'db_master_can_read': True}
"""

classifiers = """\
Development Status :: 4 - Beta
License :: OSI Approved :: Apache Software License
License :: OSI Approved :: BSD License
Natural Language :: English
Programming Language :: Python :: 2.6
Programming Language :: Python :: 2.7
Topic :: Utilities
"""

setup(name='PyMongo-Frisk',
      version='0.0.6',
      description='Wrapper for PyMongo Connection that offers additional application level monitoring. When connecting to multiple MongoDB servers, allows you to verify connectivity to MongoDB slave servers.',
      author='Brian Richardson & James Townley',
      author_email='jtownley@point2.com',
      url='http://www.point2.com/',
      py_modules=['pymongo_frisk'],
      install_requires=['pymongo==1.9'],
      license ='Release under Apache-BSD style license',
      platforms = ["any"],
      classifiers = filter(None, classifiers.split("\n")),
      long_description = __doc__
     )
