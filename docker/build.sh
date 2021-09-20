#!/bin/bash
cd $(dirname $0)
source env.sh
cd ..
docker rmi labteral/stopover-python:$VERSION 2> /dev/null
docker build -t labteral/stopover-python:$VERSION -f docker/Dockerfile .
