from pyspark import pipelines as dp
from pyspark.sql.functions import *

rules = {"rule1" : "product_id IS NOT NULL",
         "rule2" : "updated_at IS NOT NULL",
         "rule3" : "product_name IS NOT NULL"}

@dp.table(name = "products_table")
@dp.expect_all(rules)
def products_table():
    df = spark.read.table("project.source.products")
    return df