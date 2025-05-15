# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, lower, broadcast, col, collect_list, collect_set, array_contains, size


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


class SecondTransformation(Transformation):
    """ 
        Customer who have bought both airpods and iphone
    """

    def CustomerAirpodsIphone(self, inputDfs):

        transactiondf = inputDfs.get('transactionDF')
        customerdf = inputDfs.get('customerDF')
        
        transformedDF = transactiondf.groupBy(col("customer_id")).agg(collect_set(col("product_name")).alias("product_set"))
        
        transformedDF = transformedDF.filter((size(col("product_set")) == 2) & 
                                            array_contains(col("product_set"), "iPhone") &
                                            array_contains(col("product_set"), "AirPods")
                                            )
        
        joinDF = transformedDF.join( customerdf, "customer_id" ).select(col("customer_name"), col("product_set"))
        joinDF.show()

        return joinDF




# COMMAND ----------

