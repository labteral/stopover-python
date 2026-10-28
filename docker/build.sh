#!/bin/bash
set -euo pipefail
cd $(dirname $0)
source env.sh
cd ..
# PEP 440 local versions carry '+', which is not valid in a Docker tag.
TAG=$(echo "$VERSION" | tr '+' '-')
docker build -t labteral/stopover-python:$TAG -f docker/Dockerfile .
