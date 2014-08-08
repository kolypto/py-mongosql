#!/usr/bin/env python
""" SqlAlchemy queries with MongoDB-style """

from setuptools import setup, find_packages

setup(
    name='mongosql',
    version='1.0.2-0',
    author='Mark Vartanyan',
    author_email='kolypto@gmail.com',

    url='https://github.com/kolypto/py-mongosql',
    license='BSD',
    description=__doc__,
    long_description=open('README.rst').read(),
    keywords=['sqlalchemy'],

    packages=find_packages(),
    scripts=[],
    entry_points={},

    install_requires=[
        'sqlalchemy >= 0.9.0',
    ],
    extras_require={
        '_tests': ['nose', 'psycopg2', 'flask-jsontools'],
    },
    include_package_data=True,
    test_suite='nose.collector',

    platforms='any',
    classifiers=[
        # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        #'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
