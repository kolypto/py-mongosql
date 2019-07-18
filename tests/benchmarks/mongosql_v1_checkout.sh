#!/bin/bash
set -xe

cd $(dirname "$0")
git clone --depth 1  --branch v1.5 git@github.com:kolypto/py-mongosql.git mongosql_v1
touch mongosql_v1/__init__.py
