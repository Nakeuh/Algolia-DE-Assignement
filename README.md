# Algolia-DE-Assignement

This repository's objective is to complete a Data Engineering Assignement as defined in `Algolia-Data_Engineer_Assignment.pdf`.

`docker-compose.yaml` file has been retrieved from [official Airflow repository](https://github.com/apache/airflow/blob/main/docs/apache-airflow/howto/docker-compose/docker-compose.yaml).
## Prerequisites

To use this project, you must have installed : 
* [Docker Compose](https://docs.docker.com/compose/install/)

You will also need AWS credentials.

## Build 

In project's folder, run 

```console
> docker compose up
```

## Check Environement

### Airflow

To check if Airflow is properly running, open a browser at `http://localhost:8080/home`

Default credentials should be `airflow`:`airflow`.

### PostgreSQL

To check if Postgres is running and data are loaded, run the following commands : 

```console
# PREFIX depends on the name of the parent folder when you ran docker compose. You can list containers with `docker ps -a` 
> docker exec -it [PREFIX]-postgres-1 bash 
> psql -h localhost -U airflow

# List all databases on postgres
> \l 
```
## Configure

### Credentials 
To retrieve data from AWS bucket, and store data to postgres you will need to configure credentials in Airflow.

To do so, setup variables in Airflow UI : 
![alt text](doc/images/setup_variable.png)

Note : we should be able to access public bucket without credentials, but for some reasons it is not working at the time I write this README.

## Run the assignement

Once the environment is built and configured, you can run the pipeline (finally !).

Open a browser at `http://localhost:8080/home` and Unpause "`shopify_configuration_2_postgres`" DAG.

#### Expected outcome : 
* Airflow should trigger 7 runs of the pipeline (one after the other)
* One of them (start date = 2019-04-03) should have all it's steps as skipped because the input file is missing for this date

![alt text](image.png)
* Data are properly inserted in Postgres : 

```console
> docker exec -it algolia-de-assignement-postgres-1 bash
> psql -h localhost -U airflow
psql (13.16 (Debian 13.16-1.pgdg120+1))
Type "help" for help.

> airflow=# \c postgres 
You are now connected to database "postgres" as user "airflow".
> postgres=# select count(*) from "shopify_configuration_2019-04-02";
 count 
-------
  1142
(1 row)

> postgres=# select id, shop_domain, has_specific_prefix from "shopify_configuration_2019-04-02" LIMIT 5;
                  id                  |               shop_domain               |        index_prefix         | has_specific_prefix 
--------------------------------------+-----------------------------------------+-----------------------------+---------------------
 1d948de7-ea94-4e00-b784-1b598c97dbaa | himself-hair-southern.myshopify.com     | shopify_already_            | t
 9e1209d3-6a7d-45fa-b330-a452316f9dcc | professional-really-music.myshopify.com | shopify_                    | f
 bceefd16-ebb1-4a26-9080-38cd65b6f5ae | movie-than-about.myshopify.com          | shopify_                    | f
 67bc3189-e1b1-4628-beb6-4dc288729c20 | growth-this-identify.myshopify.com      | shopify_his_chance_husband_ | t
 fc91131e-1175-49a5-be3d-514816f365fd | do-forget-majority.myshopify.com        | shopify_behind_             | t
(5 rows)

```

* If you re-trigger the pipeline (remove existing runs for the DAG and hit the "Reparse DAG" button), everything should work fine again, the data will get processed and replace the existing data in the database

## Run tests

On Root folder, run 
```console
> pytest .
```

Python and pytest must be installed before