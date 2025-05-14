# Databricks notebook source
# import Reader_Factory
# %run "./Reader_Factory"

class DataExtractor:
    """
    Implement steps for extract or read the data 
    """
    def extract(self):
        customerInputDF = (
                            GetData().
                            get_data_source(
                                            data_type = 'csv', 
                                            path = "/FileStore/tables/Apple_Data/Customer_Updated.csv")
                            .get_data_frame()
                            )
        
        transactionInputDF =(
                            GetData.
                            get_data_source(
                                            data_type= 'csv',
                                            path= "/FileStore/tables/Apple_Data/Transaction_Updated.csv")
                            .get_data_frame() 
                            )
        
        InputDFs = {'customerInputDF': customerInputDF,
                    'transactionInputDF': transactionInputDF }
        
        return InputDFs

# COMMAND ----------

