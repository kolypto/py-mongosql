all:

SHELL := /bin/bash

# Package
.PHONY: clean
clean:
	@rm -rf build/ dist/ *.egg-info/
README.md: $(shell find mongosql/) $(wildcard misc/_doc/**)
	@python misc/_doc/README.py | j2 --format=json -o README.md misc/_doc/README.md.j2

.PHONY: build publish-test publish
build: README.md
	@./setup.py build sdist bdist_wheel
publish-test: README.md
	@twine upload --repository pypitest dist/*
publish: README.md
	@twine upload dist/*


.PHONY: test test-tox test-profile
test:
	@# Before testing, run: $ docker-compose up -d
	@nosetests tests/
test-tox:
	@tox
test-profile:
	@nosetests --with-cprofile --cprofile-stats-file=profile.out tests/
	@gprof2dot -f pstats profile.out | dot -Tpng -o profile.png
