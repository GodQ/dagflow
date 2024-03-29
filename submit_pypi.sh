#!/usr/bin/env bash


# pip install --upgrade twine
rm -rf dist build dagflow.egg-info
python setup.py sdist bdist
rm dist/*.zip
twine upload dist/*

# test
twine upload dist/* --repository-url https://test.pypi.org/legacy/
pip install -U dagflow --index-url https://test.pypi.org/simple/