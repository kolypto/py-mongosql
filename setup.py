#!/usr/bin/env python
""" A JSON query engine with SqlAlchemy as a back-end """

import sys
from setuptools import setup, find_packages

if not sys.version_info >= (3, 6, 0):
    raise ImportError('MongoSQL 2.0 only supports Python 3.6+')


setup(
    name='mongosql',
    version='2.0.9',
    author='Mark Vartanyan',
    author_email='kolypto@gmail.com',

    url='https://github.com/kolypto/py-mongosql',
    license='BSD',
    description=__doc__,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    keywords=['sqlalchemy'],

    packages=find_packages(exclude=('tests',)),
    scripts=[],
    entry_points={},

    install_requires=[
        'sqlalchemy >= 1.2.0,!=1.2.9',
    ],
    extras_require={},
    include_package_data=True,
    test_suite='nose.collector',

    platforms='any',
    classifiers=[
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
