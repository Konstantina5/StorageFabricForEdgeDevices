#!/bin/bash

docker build -t edge-sqlite-db:1.0.0 .
docker-compose -f docker-orders-compose.yaml up --build

# detached
# docker-compose -f docker-orders-compose.yaml up --build -d