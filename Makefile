COMPOSE = docker compose

# Core
build:
  $(COMPOSE) build

up:
  $(COMPOSE) up -d

up-build:
  $(COMPOSE) up -d --build

down:
  $(COMPOSE) down

# Logs

logs:
  $(COMPOSE) logs -f

logs-worker:
  $(COMPOSE) logs -f airflow-worker

logs-scheduler:
  $(COMPOSE) logs -f airflow-scheduler

#  Utilities 

restart-worker:
  $(COMPOSE) restart airflow-worker

psql:
  docker exec -it pipeline_data_db psql -U geouser -d geopipeline

# Destructive 

down-volumes:
  $(COMPOSE) down --volumes --remove-orphans

kill:
  @echo "WARNING: removes all containers, volumes, and images."
  @printf "Continue? [y/N] " && read c && [ "$$c" = y ] || [ "$$c" = Y ] || (echo "Aborted."; exit 1)
  $(COMPOSE) down --volumes --remove-orphans --rmi all