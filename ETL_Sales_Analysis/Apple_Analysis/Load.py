# Databricks notebook source
# %run "./Loader_Factory"
import Loader_Factory

class Loader:
    """ Abstract Class """
    def __init__(self, transformeddf):
        self.transformeddf = transformeddf

    def load(self):
        pass

class loadAirpodsAfterIphone(Loader):

    def load(self):
        writeData(load_type = "dbfs", 
                  path = "dbfs:/FileStore/tables/Apple_Data/Output", 
                  df = self.transformeddf, 
                  writemode = "overwrite" )\
        .save_data_frame()
        

# COMMAND ----------

