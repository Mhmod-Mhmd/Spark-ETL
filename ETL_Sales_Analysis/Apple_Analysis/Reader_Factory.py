# Databricks notebook source

class DataSource:
    """ abstrac Class """

    def __init__(self, path):
        self.sourcePath = path
    
    def get_data_frame(self):
        """ Method should be implemented in subclasses """
        
        raise NotImplementedError("Subclasses must implement get_source_data()")

class CSV_DataSource(DataSource):

    def get_data_frame(self):
        return (spark.
                read.
                format('csv').
                option('header','True').
                load(self.sourcePath))

class parquet_DataSource(DataSource):
    
    def get_data_frame(self):
        return (spark.
                read.
                format('parquet').
                table(self.sourcePath 
        ))

class delta_DataSource(DataSource):

    def get_data_frame(self):
        return (spark.
                read.
                format('delta').
                option('header','True').
                load(self.sourcePath))
        

class GetData:

    @staticmethod
    def get_data_source(data_type, path):
        if data_type == 'csv':
            return CSV_DataSource(path)
        elif data_type == 'parquet':
            return parquet_DataSource(path)
        elif data_type == 'delta':
            return delta_DataSource(path)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")



# COMMAND ----------

