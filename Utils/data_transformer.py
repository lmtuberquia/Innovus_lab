from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit

class DataTransformer:
    """
        A class to transform data using PySpark.

        Attributes:
            None

        Methods:
            transform_data(data, processing_date, file_name): Transforms the input data.
            close(): Closes the SparkSession and cleans up resources.
        """
    def transform_data(self, data, processing_date, file_name):
        """
                Transforms the input data using PySpark.

                Args:
                    data (list): The input data as a list of dictionaries.
                    processing_date (str): The processing date.
                    file_name (str): The name of the file.

                Returns:
                    pyspark.sql.DataFrame: The transformed DataFrame.
        """

        spark = SparkSession.builder.getOrCreate()

        df = spark.createDataFrame(data)

        # Explode the orders column to flatten it
        exploded_df = df.withColumn("order", explode("orders"))

        # Add columns for processing date and file name
        df_with_metadata = exploded_df.withColumn("processingDate", lit(processing_date)).withColumn("fileName", lit(file_name))

        # Select all the required columns
        transformed_df = df_with_metadata.select(
            "customerId",
            "name",
            "contactName",
            "country",
            "accountCreated",
            col("order.orderId").alias("orderId"),
            col("order.orderDate").alias("orderDate"),
            "processingDate",
            "fileName"
        )

        return transformed_df

    def close(self):
        """
                Closes the SparkSession and cleans up resources.
        """
        # Clean up resources
        SparkSession.builder.getOrCreate().stop()
