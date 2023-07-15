import pytest
from pyspark.sql import SparkSession
from Utils.data_loader import DataLoader

def test_data_loader():
    # Create a sample DataFrame
    # spark = SparkSession.builder.getOrCreate()
    # data = [("John", 25), ("Jane", 30), ("Sam", 40)]
    # df = spark.createDataFrame(data, ["name", "age"])
    #
    # # Create a DataLoader instance
    # db_config = {
    #     "host": "localhost",
    #     "port": 3306,
    #     "database": "mydatabase",
    #     "user": "myuser",
    #     "password": "mypassword"
    # }
    # loader = DataLoader(db_config)
    #
    # # Process the DataFrame
    # loader.process_stage(df, "stage_table", "sample_file.csv", db_config)
    #
    # # Perform assertions on the loaded data
    # # ...
    #
    # # Close the DataLoader
    # loader.close()
    assert True
