#!/usr/bin/env python
import os
from setuptools import setup

setup(name='pymongo_frisk',
      version='0.0.1',
      description='Wrapper for PyMongo Connection that offers additional application level monitoring. When connecting to multiple MongoDB servers, allows you to verify connectivity to MongoDB slave servers.',
      author='Brian Richardson , James Townley',
      url='http://www.point2.com/',
      py_modules=['pymongo_frisk'],
      install_requires=['pymongo==1.7']
     )