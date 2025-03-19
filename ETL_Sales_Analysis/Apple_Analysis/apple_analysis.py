# Databricks notebook source
# MAGIC %md
# MAGIC Customer who have bought airpods after buying iphone
# MAGIC Customer who have bought both airpods and iphone
# MAGIC list all product bought by customer

# COMMAND ----------

# MAGIC %run "./Reader_Factory"

# COMMAND ----------

# MAGIC %run "./Loader_Factory"

# COMMAND ----------

class DataExtractor:
    """
    Implement steps for extract or read the data 
    """
    def extract(self):
        customerInputDF = (
                            GetData().
                            get_data_source(
                                data_type = 'csv', 
                                path = "/FileStore/tables/Apple_Data/Customer_Updated.csv").
                            get_data_frame()
                            )
        
        transactionInputDF =(
                            GetData.
                            get_data_source(data_type= 'csv',
                                             path= "/FileStore/tables/Apple_Data/Transaction_Updated.csv").
                            get_data_frame() 
                            )
        
        InputDFs = {'customerInputDF': customerInputDF,
                    'transactionInputDF': transactionInputDF }
        
        return InputDFs

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast

class AirPodsAfterIphoneTransformer:
    """
        Customers who have bought AirPods after buying Iphone 
    """
    def __init__(self):
        pass

    def transform(self, Input_DFs):
        
        transaction_df = Input_DFs.get('transactionInputDF')
        customer_df = Input_DFs.get('customerInputDF')

        windowSpec = Window.orderBy('transaction_date').partitionBy('customer_id')
        transaction_df = transaction_df.withColumn('next_product_name', lead('product_name').over(windowSpec))
        transaction_df_filtered = transaction_df.filter((col('next_product_name') == 'AirPods') & (col('product_name') == 'iPhone' ))
        print('Filtered Transaction DF')
        transaction_df_filtered.show()

        transaction_df_filtered = transaction_df_filtered.withColumn("customer_id", col("customer_id").cast("string"))
        customer_df = customer_df.withColumn("customer_id", col("customer_id").cast("string"))

        # Perform the join
        joined_dfs = customer_df.join(
                                    broadcast(transaction_df_filtered), 
                                    on="customer_id", 
                                    how="inner")

        # Show the result
        print('Joined DataFrams [customer_DF, transaction_df]')
        joined_dfs.show()

        # selected Tables
        print('Selected Tables')
        selected_tables = joined_dfs.select(['customer_name', 'transaction_date', 'join_date','location'])
        selected_tables.show()

        return selected_tables




# COMMAND ----------

class AbstractLoader:
    """ 
    Abstract Class
    """
    def __init__(self, TransFormedDF):
        self.TransFormedDF = TransFormedDF



class SaveAirpodsAfterIphoneDF(AbstractLoader):


    def Load(self):
        """
        Save Data frame without partition
        """

        writeData(
            load_type = 'dbfs',
            df = self.TransFormedDF,
            path = 'dbfs:/FileStore/tables/Apple_Data/Output',
            writemode = 'overwrite').save_data_frame()



        


# COMMAND ----------

df_read = spark.read.parquet("dbfs:/FileStore/tables/Apple_Data/Output")
df_read.show()


# COMMAND ----------


class WorkFow:

    def __init__(self):
        pass

    def runner(self):
        
        # Step 1: Extract all required Data from different sources
        InputDFs = DataExtractor().extract()
        
        # Step 2: Transform Data for Customers who have bought AirPods after buying Iphone
        TransFormedDF = AirPodsAfterIphoneTransformer().transform(InputDFs)

        # Step 3: Load Data to different Sources
        SaveAirpodsAfterIphoneDF(TransFormedDF).Load()
    


workflow = WorkFow()
workflow.runner()

# COMMAND ----------




# COMMAND ----------

customer_df = (spark.
                  read.
                  format('csv').
                  option('header','True').
                  load('dbfs:/FileStore/tables/Apple_Data/Customer_Updated.csv')
)

transaction_df = (spark.
                  read.
                  format('csv').
                  option('header','True').
                  load('dbfs:/FileStore/tables/Apple_Data/Transaction_Updated.csv')
)


customer_df.orderBy('customer_id', 'customer_id').show()

# COMMAND ----------



# COMMAND ----------

