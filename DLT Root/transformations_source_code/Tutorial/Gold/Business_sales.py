import dlt

# Creating a MAT BUSINESS VIEW
from pyspark.sql.functions import *
from pyspark.sql.types import *
@dlt.table(
    name="business_sales"
)
def business_sales():
    df_fact= spark.read.table("Fact_sales")
    df_dimCust=spark.read.table("dim_customers")
    df_dimProd=spark.read.table("dim_products")

    df_join= df_fact.join(df_dimCust,df_fact.customer_id==df_dimCust.customer_id,how="inner").join(df_dimProd,df_fact.product_id==df_dimProd.product_id,"inner")

    df_prun= df_join.select("region","category","total_amount")
    df_agg= df_prun.groupby("region","category").agg(sum("total_amount").alias("Total_sales"))

    return df_agg
