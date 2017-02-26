#!/usr/bin/env bash

set -e

cd $(dirname $0)

if [ "$TRAVIS_BRANCH" == "release" ]
then

    pip install twine --upgrade
    python setup.py sdist
    twine upload dist/*

fi
