#!/bin/bash

BASEDIR=$(dirname "$0")

# remove infrastructure
docker-compose -f $BASEDIR/docker-compose.yml down
# remove <none> images
docker image prune