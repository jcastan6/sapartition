#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='sapartition',
      version='0.0.7',
      description='Table partition and query splitting for SQLAlchemy',
      packages=['sapartition'],
      install_requires=[
          'SQLAlchemy>=0.7.0',
      ],
)
