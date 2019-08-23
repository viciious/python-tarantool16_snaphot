PYTHON ?= python2.7

default:
	$(PYTHON) setup.py build

install:
	$(PYTHON) setup.py install

test:
	$(PYTHON) setup.py test

