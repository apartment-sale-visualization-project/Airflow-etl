# Airflow-etl

source .env
docker-compose --env-file ./.env -f ./postgres-docker-compose.yaml up -d
docker-compose -f airflow-docker-compose.yaml up airflow-init
docker-compose -f airflow-docker-compose.yaml up -d
