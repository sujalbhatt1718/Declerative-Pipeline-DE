import dlt

#create empty streaming table
dlt.create_streaming_table(
    name= "Fact_sales"
)

# Auto cdc flow

dlt.create_auto_cdc_flow(
    target = "Fact_sales",
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