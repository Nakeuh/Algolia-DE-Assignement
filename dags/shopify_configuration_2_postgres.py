# The Integration team has deployed a cron job to dump a CSV file containing all the new Shopify configurations daily at 2 AM UTC.


from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sink import postgres_sink
from source import s3_source
from io import StringIO

logger = logging.getLogger(__name__)

s3_access_key = Variable.get("s3_access_key")
s3_secret_key = Variable.get("s3_secret_key")

postgres_user = Variable.get("postgres_user")
postgres_password = Variable.get("postgres_password")

s3_client = s3_source.get_s3_client(s3_access_key, s3_secret_key)
sql_engine = postgres_sink.get_sql_engine(
    f"postgresql://{postgres_user}:{postgres_password}@postgres:5432/postgres"
)

# Note : data are shared between tasks uing XCOM.
# I think it would make more sense to store/read data from/to S3 but I don't have one I can write in available.
# If I had more time, I would include a MinIO instance in the docker compose and would not use XCOM to share shopify configurations


# Function that parses data as csv, filters out empty application_id, and add a has_specific_prefix column
def transform_data(ti=None, **kwargs) -> str:
    data_attribute_name = kwargs["data_attribute_name"]
    data_task_id = kwargs["data_task_id"]
    csvStringIO = StringIO(ti.xcom_pull(key=data_attribute_name, task_ids=data_task_id))
    df = pd.read_csv(csvStringIO, sep=",", header=0)
    logger.info(f"{len(df.index)} lines loaded.")

    # Filter out each row with empty application_id
    df = df[df["application_id"].str.len() > 0]
    logger.info(f"{len(df.index)} lines left after filtering empty application ids")

    # Add a has_specific_prefix column set to true if the value of index_prefix differs from shopify_ else to false
    df["has_specific_prefix"] = np.where(df["index_prefix"] != "shopify_", True, False)

    # Add a has_specific_prefix column set to true if the value of index_prefix differs from shopify_ else to false
    df["index_column"] = df["id"] + df["export_date"]

    ti.xcom_push(key="transformed_data", value=df.values.tolist())
    ti.xcom_push(key="transformed_data_columns", value=df.columns.to_list())


default_args = {"retries": 2, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="shopify_configuration_2_postgres",
    default_args=default_args,
    description="Download Shopify configuration from 2019-04-01 to 2019-04-07 and load them to postgres",
    start_date=datetime(2019, 4, 1),
    end_date=datetime(2019, 4, 7),
    schedule_interval="0 3 * * * ",
    catchup=True,
    max_active_runs=1,
) as dag:
    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=s3_source.get_data_func,
        op_kwargs={
            "s3_client": s3_client,
            "bucket_name": "alg-data-public",
            "filepath": "{{ds}}.csv",
        },
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={
            "data_task_id": "get_data",
            "data_attribute_name": "data",
        },
    )

    store_data_task = PythonOperator(
        task_id="store_data",
        python_callable=postgres_sink.insert_records,
        op_kwargs={
            "engine": sql_engine,
            "table": "shopify_configuration",
            "index_label": "export_date",
            "primary_key":"index_column",
            "data_task_id": "transform_data",
            "data_attribute_name": "transformed_data",
            "columns_attribute_name": "transformed_data_columns",
        },
    )

    get_data_task >> transform_data_task >> store_data_task
