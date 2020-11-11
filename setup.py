#!/usr/bin/env python
""" A JSON query engine with SqlAlchemy as a back-end """

from setuptools import setup, find_packages

setup(
    name='mongosql',
    version='2.0.12',
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

    python_requires='>= 3.6',
    install_requires=[
        'sqlalchemy >= 1.2.0,!=1.2.9',
        'nplus1loader',
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
