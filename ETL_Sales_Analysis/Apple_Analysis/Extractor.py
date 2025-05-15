# Databricks notebook source

# %run "./Reader_Factory"
import Reader_Factory

class DataExtractor:
    """
    Implement steps for extract or read the data 
    """
    def extract(self):
        try:
            customer_DF = (
                            GetData().
                            get_data_source(
                                            data_type = 'csv', 
                                            path = "/FileStore/tables/Apple_Data/Customer_Updated.csv")
                            .get_data_frame()
                                )
            
            transaction_DF = (
                                GetData.
                                get_data_source(
                                                data_type= 'csv',
                                                path= "/FileStore/tables/Apple_Data/Transaction_Updated.csv")
                                .get_data_frame() 
                                )
            
            products_DF = (
                            GetData
                            .get_data_source('csv', "/FileStore/tables/Apple_Data/Products_Updated.csv")
                            .get_data_frame() )
            
            InputDFs = {'customerDF': customer_DF,
                        'transactionDF': transaction_DF,
                        'productDF': products_DF}
            
        except Exception as e:
            print(f"An Error occured while Extracting data in extract() method : {e}")

        else:
            return InputDFs
        

# s = DataExtractor()
# dfs = s.extract()
# dfs['productDF'].show()

# COMMAND ----------

