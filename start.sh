#!/bin/bash

docker start etl-zookeeper
echo "Zookeeper started!"

docker start etl-mongo
echo "Mongo started!"

docker start etl-postgres
echo "Postgres started!"

sleep 20s
docker start etl-kafka-broker
echo "Kafka started!"

echo "ETL Containers started successfully!"