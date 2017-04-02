#!/usr/bin/env bash

set -e

cd $(dirname $0)

if [ -n "$TRAVIS_TAG" ]
then

    pip install twine --upgrade
    python setup.py sdist
    twine upload -u ${TWINE_USER} -p ${TWINE_PWD} -r pypi dist/*

fi
