#!/bin/sh
if [! -d logs]; then
    mkdir ./logs
    chmod -R 777 ./logs
fi
if [! -d plugins]; then
    mkdir ./plugins
    chmod -R 777 ./plugins
fi
docker-compose --env-file ./.env -f ./postgres-docker-compose.yaml up -d
docker-compose -f airflow-docker-compose.yaml up -d