# import dlt

# #create streaming Table

# @dlt.table(
#   name="first_stream_table")

# def first_stream_table():
#     df = spark.readStream.table("dlsujal.source.orders")
#     return df

# # create Materialized Table
# @dlt.table(
#   name="first_materialized_table")

# def first_materialized_table():
#     df = spark.read.table("dlsujal.source.orders")
#     return df

# #create batch view
# @dlt.view(
#     name="first_bTCH_table"
# )
# def first_batch_table():
#     df = spark.read.table("dlsujal.source.orders")
#     return df

# #create streaming view
# @dlt.view(
#     name="first_stRM_table"
# )
# def first_stream_table():
#     df = spark.readStream.table("dlsujal.source.orders")
#     return df
