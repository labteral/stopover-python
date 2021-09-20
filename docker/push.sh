#!/bin/bash
cd $(dirname $0)
source env.sh
docker push labteral/stopover-python:$VERSION
