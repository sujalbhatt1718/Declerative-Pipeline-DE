import dlt
# Sales Expectation 
sales_rules = {
    "rule_1":"sales_id IS NOT NULL"
}

# Empty string
dlt.create_streaming_table(
    name="sales_stg",
    expect_all_or_drop = sales_rules

)

#creating East Sales Flow
@dlt.append_flow(target="sales_stg")
def east_sales():
    df= spark.readStream.table("dlsujal.source.sales_east")
    return df

#Creating West Sales FLow
@dlt.append_flow(target="sales_stg")
def west_sales():
    df= spark.readStream.table("dlsujal.source.sales_west")
    return df