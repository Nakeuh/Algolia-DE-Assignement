import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_sql_engine(url: str):
    return create_engine(url)


# Function to insert all records into PostgreSQL
# expected kwargs :
#   - sql_engine : the sql alchemy engine that will be used to insert data
#   - table : the table name to write data into
#   - data_task_id : the id of the task producing the data to insert (through XCOM)
#   - data_attribute_name : the attribute name to retrieve the data (as list) (through XCOM)
#   - columns_attribute_name : the attribute name to retrieve the column names (through XCOM)
#   - if_exists : define the behavior if the table already exists{‘fail’, ‘replace’, ‘append’}, default ‘fail’
def insert_records(ti, **kwargs):
    engine = kwargs["sql_engine"]
    table = kwargs["table"]
    data_task_id = kwargs["data_task_id"]
    data_attribute_name = kwargs["data_attribute_name"]
    column_attribute_name = kwargs["columns_attribute_name"]

    if_exists = None
    if "if_exists" in kwargs:
        if_exists = kwargs["if_exists"]

    columns = ti.xcom_pull(key=column_attribute_name, task_ids=data_task_id)
    data = ti.xcom_pull(key=data_attribute_name, task_ids=data_task_id)

    df_data = pd.DataFrame(data, columns=columns)
    df_data.to_sql(name=table, con=engine, if_exists=if_exists, index=False)

    logger.info(f"{len(df_data.index)} rows inserted in database")
