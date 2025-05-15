# Databricks notebook source

# %run "./Extractor"
# %run "./Transformer"
# %run "./Loader"

import Extractor
import Transformer
import Loader

class FirstWorkFow:

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


class SecondWorkflow:
        def __init__(self):
            pass
        def runner(self):
        
            # Step 1: Extract all required Data from different sources
            dfs = DataExtractor().extract()

            # Step 2: Transform Data for Customers who have bought AirPods after buying Iphone
            transformedData = SecondTransformation().CustomerAirpodsIphone(dfs)
            # transformedData.show()

            # Step 3: Load Data to different Sources
            loadAirpodsAfterIphone(transformedData).load()


class WorkFlowRun:
    def __init__(self, transformation_name):
        self.transformation_name = transformation_name

    def run(self):
        if self.transformation_name == "airpodsafteriphone":
            return FirstWorkFow().runner()
        elif self.transformation_name == "airpodswithiphone":
            return SecondWorkflow().runner()
        else:
            print("this is not defined as a transformed")


trans_name = 'airpodswithiphone'
WorkFlowRun(trans_name).run()

# COMMAND ----------
