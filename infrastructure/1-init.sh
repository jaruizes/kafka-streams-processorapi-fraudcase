#!/bin/bash

BASEDIR=$(dirname "$0")

# build and run movements (fraud) generator process
docker build -t fraud-checker-generator:v1 $BASEDIR/fraud-checker-generator/

# build infrastructure
docker-compose -f $BASEDIR/docker-compose.yml up -d