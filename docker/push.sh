#!/bin/bash
VERSION=$(cat ../stopover/__init__.py | grep version | cut -d\' -f2)
docker push labteral/stopover-python:$VERSION
