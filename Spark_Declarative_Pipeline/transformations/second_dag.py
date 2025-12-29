from pyspark import pipelines as dp
from pyspark.sql.functions import *

## Idempotency means exactly once
## For streaming tables behaviour is append only
## For aggregating or grouping we have to go with materialized view.l
## we can create a materialized view on top of streaming table ,but reverse is not possible
## Materialized View
@dp.table(name = "src_sales_stream")
def src_sales():
    df = spark.readStream.table("project.source.sales")
    df = df.withColumn("date", to_date(col("date") , 'yyyy-MM-dd'))
    return df

## Materialized View referring to another materialized view
@dp.table(name = "enr_sales_stream")
def enr_sales():
    df = spark.readStream.table("src_sales_stream")
    df = df.withColumn("revenue",col("revenue") * 1.05)
    return df

## Materialized View referring to another materialized view
@dp.table(name = "cur_sales_stream")
def cur_sales():
    df = spark.readStream.table("enr_sales_stream")
    df = df.groupBy("date").agg(sum("revenue").alias("total_revenue"))
    return df


                      