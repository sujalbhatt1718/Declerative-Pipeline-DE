import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
# Transforming Sales Data
@dlt.view(
    name="sales_enr_view"
)
def sales_stg_trns():
    df= spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount",col("quantity")*col("amount"))
    return df
    
# Crerating Destination Silver Table
dlt.create_streaming_table(
    name="sales_enr"
) 

dp.create_auto_cdc_flow(
    target = "sales_enr",
    source = "sales_enr_view",
    keys = ["sales_id"],
    sequence_by = "sale_timestamp",

    system_sequence_by = None, # optional
    ignore_null_updates = False, # optional
    ignore_null_updates_column_list = None, # optional
    ignore_null_updates_except_column_list = None, # optional
    columns_to_update = None, # optional
    apply_as_deletes = None, # optional
    apply_as_truncates = None, # optional
    column_list = None, # optional
    except_column_list = None, # optional
    stored_as_scd_type = 1, # optional
    track_history_column_list = None, # optional
    track_history_except_column_list = None, # optional
    name = None, # optional
    once = False # optional
)
