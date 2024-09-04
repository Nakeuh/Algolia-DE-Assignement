import pandas as pd
import numpy as np
import logging
from io import StringIO

logger = logging.getLogger(__name__)

# Function that parses data as csv, filters out empty application_id, and add a has_specific_prefix column
# expected kwargs :
#   - data_task_id : the id of the task producing the data to transform (through XCOM)
#   - data_attribute_name : the attribute name to retrieve the data to transform (as list) (through XCOM)
# return (XCOM):
#   - transformed_data: the transformed data as a list of rows
#   - transformed_data_columns: the column names as a list of string
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

    ti.xcom_push(key="transformed_data", value=df.values.tolist())
    ti.xcom_push(key="transformed_data_columns", value=df.columns.to_list())
