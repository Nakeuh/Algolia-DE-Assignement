# The Integration team has deployed a cron job to dump a CSV file containing all the new Shopify configurations daily at 2 AM UTC.

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
from airflow.operators.python import PythonOperator
from pipelines.shopify_configuration_2_postgres.utils import utils
from sink import sql_sink
from source import s3_source
from airflow.models import Variable

logger = logging.getLogger(__name__)

# TODO : In this DAG data are shared between tasks uing XCOM.
# I think it would make more sense to store/read data from/to S3 but I don't have a bucket I can write in available.
# With more time, I would include a MinIO instance in the docker compose and would not use XCOM to share shopify configurations


# TODO : Refactor this bad practice : usage of variable in top level code
s3_access_key = Variable.get("s3_access_key")
s3_secret_key = Variable.get("s3_secret_key")
postgres_user = Variable.get("postgres_user")
postgres_password = Variable.get("postgres_password")
s3_client = s3_source.get_s3_client(s3_access_key, s3_secret_key)
sql_engine = sql_sink.get_sql_engine(
    f"postgresql://{postgres_user}:{postgres_password}@postgres:5432/postgres"
)


with DAG(
    dag_id="shopify_configuration_2_postgres",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    description="Download Shopify configuration from 2019-04-01 to 2019-04-07 and load them to postgres",
    start_date=datetime(2019, 4, 1),
    end_date=datetime(2019, 4, 8),
    schedule_interval="0 3 * * * ", 
    catchup=True,
    max_active_runs=1,  # We don't want parallel inserts in Postgres
) as dag:

    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=s3_source.get_data,
        op_kwargs={
            "s3_client": s3_client,
            "bucket_name": "alg-data-public",
            "filepath": "{{ds}}.csv",
        },
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=utils.transform_data,
        op_kwargs={
            "data_task_id": "get_data",
            "data_attribute_name": "data",
        },
    )

    store_data_task = PythonOperator(
        task_id="store_data",
        python_callable=sql_sink.insert_records,
        op_kwargs={
            "sql_engine": sql_engine,
            "table": "shopify_configuration_{{ds}}",
            "if_exists": "replace",
            "data_task_id": "transform_data",
            "data_attribute_name": "transformed_data",
            "columns_attribute_name": "transformed_data_columns",
        },
    )

    get_data_task >> transform_data_task >> store_data_task
