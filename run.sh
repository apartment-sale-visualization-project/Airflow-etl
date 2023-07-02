#!/bin/sh
if [ ! -d "logs" ]; then
    mkdir ./logs
    chmod -R 777 ./logs
    echo "create logs folder"
fi
if [ ! -d "plugins" ]; then
    mkdir ./plugins
    chmod -R 777 ./plugins
    echo "create dags folder"
fi
docker-compose --env-file ./.env -f ./postgres-docker-compose.yaml up -d
docker-compose -f airflow-docker-compose.yaml up -d