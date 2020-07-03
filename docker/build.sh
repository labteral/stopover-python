#!/bin/bash
VERSION=$(cat ../stopover/__init__.py | grep version | cut -d\' -f2)
docker rmi labteral/stopover-client-python:$VERSION 2> /dev/null
tar cf ../../stopover.tar ../
mv ../../stopover.tar .
docker build -t labteral/stopover-client-python:$VERSION .
rm -f stopover.tar
