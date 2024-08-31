# Algolia-DE-Assignement

This repo's objective is to complete Data Engineering Assignement as defined in `Algolia-Data_Engineer_Assignment.pdf`.

`docker-compose.yaml` file has been retrieved from [official Airflow repository](https://github.com/apache/airflow/blob/main/docs/apache-airflow/howto/docker-compose/docker-compose.yaml).
## Prerequisites

To use this project, you must have installed : 
* [Docker Compose](https://docs.docker.com/compose/install/)

## Build 

In project's folder, run 

```console
> docker compose up
```

By default, docker compose will create folders `dags`, `logs` and `plugins` as root folder. 

If needed, you can change owner of these folder using 

```console
> sudo chown [USER]:[GROUP] dags logs plugins
```

## Check Environement

### Airflow

To check if Airflow is properly running, open a browser at `http://localhost:8080/home`

Default credentials should be `airflow`:`airflow`.

### PostgreSQL

To check if Postgres is running and data are loaded, run the following commands : 

```console
> docker exec -it algolia-de-assignement-postgres-1 bash 
> psql -h localhost -U airflow

# List all databases on postgres
> \l 
```

