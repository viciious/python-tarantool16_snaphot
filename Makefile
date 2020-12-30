PYTHON ?= python2.7

default:
	$(PYTHON) setup.py build

install:
	$(PYTHON) setup.py install

bdist:
	$(PYTHON) setup.py bdist

bdist_rpm:
	$(PYTHON) setup.py bdist_rpm --python=$(PYTHON)

test:
	$(PYTHON) setup.py test

