TEST_COMMANDS := pip-setup.sh && licensecheck.sh && tox
current_dir := $(shell pwd)

.PHONY: runtests test install pep8 release clean

lint:
	@flake8 . --ignore=F403,W503 --show-source --statistics

test:
	pytest

install:
	pip install -e .
	pip install -r requirements_test.txt

release:
	@python setup.py sdist bdist_wheel

clean:
	@find . -name '*.pyc' -exec rm -f {} \;
	@find . -name '__pycache__' -exec rm -rf {} \;
	@find . -name 'Thumbs.db' -exec rm -f {} \;
	@find . -name '*~' -exec rm -f {} \;
