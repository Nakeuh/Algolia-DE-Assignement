
import pytest
import datetime
import pendulum

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import logging
import sys
import os 

sys.path.insert(0, os.path.abspath('./dags'))

from pipelines.shopify_configuration_2_postgres.utils.utils import transform_data


DATA_INTERVAL_START = pendulum.datetime(2019, 4, 1, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=3)

TEST_DAG_ID = f"my_test_dag_{int(datetime.datetime.now().timestamp())}"
DUMMY_TASK_ID = "dummy_task"
TEST_TASK_ID = "test_transform_data"

logger = logging.getLogger(__name__)


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        dummy_task = EmptyOperator(
            task_id=DUMMY_TASK_ID,
        )
        transform_data_task = PythonOperator(
            task_id=TEST_TASK_ID,
            python_callable=transform_data,
            op_kwargs={
                "data_task_id": DUMMY_TASK_ID,
                "data_attribute_name": "data",
            },
        )

    return dag


# Test function
def test_my_custom_operator_execute_no_trigger(dag):
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
        run_id=f"{TEST_DAG_ID}_nominal",
    )

    input_data = """id,shop_domain,application_id,autocomplete_enabled,user_created_at_least_one_qr,nbr_merchandised_queries,nbrs_pinned_items,showing_logo,has_changed_sort_orders,analytics_enabled,use_metafields,nbr_metafields,use_default_colors,show_products,instant_search_enabled,instant_search_enabled_on_collection,only_using_faceting_on_collection,use_merchandising_for_collection,index_prefix,indexing_paused,install_channel,export_date
id-1,house-business-growth.myshopify.com,HQRLIXFEYY,True,False,0,[0],True,False,False,False,17,True,False,False,False,False,False,shopify_within_free_ground_,False,marketplace,2019-04-01
id-2,tend-whatever-because.myshopify.com,,True,False,0,[0],True,False,False,False,20,True,False,False,False,False,False,shopify_little_different_expect_,False,marketplace,2019-04-01
id-3,animal-drive-prove.myshopify.com,143PIYP1C5,True,False,0,[0],True,False,False,False,5,True,False,False,False,False,False,shopify_,False,marketplace,2019-04-01
"""

    expected_results = pd.read_csv(f"./dags/tests/data/test_transformed_data.csv")
    expected_results_data = expected_results.values.tolist()
    expected_results_columns = expected_results.columns.tolist()

    # Dummy task used to push input data in XCOM
    dummy_ti = dagrun.get_task_instance(task_id=DUMMY_TASK_ID)
    dummy_ti.task = dag.get_task(task_id=DUMMY_TASK_ID)
    dummy_ti.xcom_push(key="data", value=input_data)

    # Ensure data are properly pushed in XCOM before testing the function
    assert dummy_ti.xcom_pull(key="data") == input_data

    # Run task to test
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    assert expected_results_columns == ti.xcom_pull(
        key="transformed_data_columns", task_ids=TEST_TASK_ID
    )
    assert expected_results_data == ti.xcom_pull(
        key="transformed_data", task_ids=TEST_TASK_ID
    )
