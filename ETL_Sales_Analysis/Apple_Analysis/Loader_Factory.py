# Databricks notebook source
class DataLoader:
    """ abstrac Class """

    def __init__(self, path, df, writemode, partition_names ):
        self.filePath = path
        self.df = df
        self.mode = writemode
        self.partition_names = partition_names

    def save_data_frame(self):
        """ Method should be implemented in subclasses """
        
        raise NotImplementedError("Subclasses must implement get_source_data()")

class SaveToDBFS(DataLoader):

    def save_data_frame(self):

        self.df.write.mode(self.mode).save(self.filePath)
        # save(self.path) save df to parquet file by default unless there is no format specified

class SaveToDBFSByPartition(DataLoader):

    def save_data_frame(self):
        
        partitions = self.partition_names 
        self.df.write.mode(self.mode.partitionBy(*partitions)).save(self.filePath)

def writeData(load_type, path, df, writemode, partition_names= None  ):

    if load_type == 'dbfs':
        return SaveToDBFS(path, df, writemode, partition_names)
    
    elif load_type == 'dbfswithpartition':
        return SaveToDBFSByPartition(path, df, writemode, partition_names)
    
    else :
        raise ValueError(f"Unsupported load type: {load_type}")

    




# COMMAND ----------

