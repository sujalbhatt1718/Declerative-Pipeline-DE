import dlt
product_rules={
    "rule_1":"product_id IS NOT NULL",
    "rule_1":"price >= 0"
}

@dlt.table(
    name="product_stg"
)
@dlt.expect_all_or_drop(product_rules)
def product_stg():
    df= spark.readStream.table("dlsujal.source.products")
    return df