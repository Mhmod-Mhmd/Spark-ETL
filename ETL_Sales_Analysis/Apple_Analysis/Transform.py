# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col

class FirstTransformer:

    def __init__(self):
        pass

    def transform(self, Input_DFs):
        
        transaction_df = self.Input_DFs.get('transactionInputDF')
        customer_df = self.Input_DFs.get('customerInputDF')

        windowSpec = Window.orderBy('transaction_date').partitionBy('customer_id')
        task1 = transaction_df.withColumn('next_product_name', lead('product_name').over(windowSpec))
        task1 = task1.filter((col('next_product_name') == 'AirPods') & (col('product_name') == 'iPhone' ))
        task1.show()

        task1 = task1.withColumn("customer_id", col("customer_id").cast("string"))
        customer_df = customer_df.withColumn("customer_id", col("customer_id").cast("string"))

        # Perform the join
        joined_df = task1.join(customer_df, on="customer_id", how="inner")

        # Show the result
        joined_df.show()


# COMMAND ----------

