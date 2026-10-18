# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
df = spark.read.table("dlsujal.source.orders")
display(df)

# COMMAND ----------

df = spark.read.table("dlsujal.source.orders")
display(df)

# COMMAND ----------

df = spark.read.table("dlsujal.transformed_orders")
display(df)

# COMMAND ----------

