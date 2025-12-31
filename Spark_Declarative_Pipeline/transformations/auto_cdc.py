from pyspark import pipelines as dp
from pyspark.sql.functions import *

## Empty streaming table SCD2
dp.create_streaming_table("products_scd2")

## Empty streaming table SCD1
dp.create_streaming_table("products_scd1")

## Streaming view source
@dp.temporary_view
def products_source():
    df = spark.readStream.table("project.source.products")
    return df

## scd_type_1
dp.create_auto_cdc_flow(
    target = "products_scd1",
    source = "products_source",
    keys = ["product_id"],
    sequence_by = col("updated_at"),
    except_column_list = ["updated_at"],
    stored_as_scd_type = "1"
)


## scd_type_2
dp.create_auto_cdc_flow(
    target = "products_scd2",
    source = "products_source",
    keys = ["product_id"],
    sequence_by = col("updated_at"),
    except_column_list = ["updated_at"],
    stored_as_scd_type = "2"
)
