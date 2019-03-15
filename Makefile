all:

SHELL := /bin/bash

# Package
.PHONY: clean
clean:
	@rm -rf build/ dist/ *.egg-info/
#README.md:
#	@python misc/_doc/README.py | j2 --format=json -o README.md misc/_doc/README.md.j2

.PHONY: build publish-test publish
build: README.rst
	@./setup.py build sdist bdist_wheel
publish-test: README.rst
	@twine upload --repository pypitest dist/*
publish: README.rst
	@twine upload dist/*


.PHONY: test test-tox
test:
	@# Before testing, run: $ docker-compose up -d
	@nosetests
test-tox:
	@tox
