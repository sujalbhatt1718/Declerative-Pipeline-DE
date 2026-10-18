import dlt

# Customers Expectation
customers_rules={
    "rule_1":"customer_id IS NOT NULL",
    "rule_1":"customer_name IS NOT NULL"
}

# Ingesting Customers
@dlt.table(
    name="customer_stg"
)
@dlt.expect_all_or_drop(customers_rules)
def customer_stg():
    df=spark.readStream.table("dlsujal.source.customers")
    return df