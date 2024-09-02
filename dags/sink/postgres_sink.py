import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from numpy import append
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_sql_engine(url: str):
    return create_engine(url)


# Function to insert records into PostgreSQL
def insert_records(ti, **kwargs):
    table = kwargs["table"]
    index_label = kwargs["index_label"]
    primary_key = kwargs["primary_key"]
    engine = kwargs["engine"]
    data_task_id = kwargs["data_task_id"]
    column_attribute_name = kwargs["columns_attribute_name"]
    data_attribute_name = kwargs["data_attribute_name"]

    columns = ti.xcom_pull(key=column_attribute_name, task_ids=data_task_id)
    data = ti.xcom_pull(key=data_attribute_name, task_ids=data_task_id)

    df_data = pd.DataFrame(data, columns=columns)
    df_data.to_sql(
        name=table, con=engine, if_exists="append", index=False, index_label=index_label
    )

    # Set up primary_key after ward as 'df.to_sql' function might create the sql table but provide no feature to configure a primary key
    # Ugly, to remove when reworking function to use postgreshook and connections
    if primary_key != None:
        try :
            with engine.connect() as con:
                con.execute(f"ALTER TABLE {table} ADD PRIMARY KEY ({primary_key});")
        except : 
            pass # The primary key as already been setup 

    # As it is, if pipeline is rerun on alredy ran data, it will fail, because of the primary key

    logger.info(f"{len(df_data.index)} rows inserted in database")
