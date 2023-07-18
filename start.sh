#!/bin/bash

docker start etl-zookeeper
docker start etl-kafka-broker
docker start etl-mongo
docker start etl-postgres

echo "ETL Containers started successfully!"