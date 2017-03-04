#!/usr/bin/env bash

set -e

cd $(dirname $0)

if [ "$TRAVIS_BRANCH" == "release" ]
then

    pip install twine --upgrade
    python setup.py sdist
    twine upload -u ${TWINE_USER} -p ${TWINE_PWD} -r pypi dist/*

fi
