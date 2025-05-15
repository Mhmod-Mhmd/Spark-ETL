# Databricks notebook source

# %run "./Extractor"
# %run "./Transformer"
# %run "./Loader"

import Extractor
import Transformer
import Loader

class WorkFow:

    def __init__(self):
        pass

    def runner(self):
        
        # Step 1: Extract all required Data from different sources
        DFs = DataExtractor().extract()
        print("Data Extracted Sucessfully!")
        
        # Step 2: Transform Data for Customers who have bought AirPods after buying Iphone
        TransFormedDF = FirstTransformation().transform(DFs)
        print(" Transformtion is done successfully")

        # Step 3: Load Data to different Sources
        SaveAirpodsAfterIphoneDF(TransFormedDF).Load()
        print(" Transformed Data Loaded Sucessfully)

        print(" ETL Pipeline Workflow completed successfully") 


if __name__ = "__main__":
    workflow = WorkFow()
    workflow.runner()

# COMMAND ----------
