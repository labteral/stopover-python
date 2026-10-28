#!/bin/bash
cd $(dirname $0)
source env.sh
# PEP 440 local versions carry '+', which is not valid in a Docker tag.
TAG=$(echo "$VERSION" | tr '+' '-')
docker push labteral/stopover-python:$TAG
