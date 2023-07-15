import argparse
from Utils.data_extractor import DataExtractor
from Utils.data_transformer import DataTransformer
from Utils.data_loader import DataLoader
from Utils.query_executor import QueryExecutor
from Settings.db_settings import DB_SETTINGS


class MainWorkflow:
    def __init__(self, date, input_path, output_table):
        """
                MainWorkflow class constructor.

                Args:
                    date (str): Date of processing the file.
                    input_path (str): Path to the input JSON data.
                    output_table (str): Name of the output stage table.
        """
        self.date = date
        self.input_path = input_path
        self.output_table = output_table
        self.data_extractor = None
        self.data_transformer = None
        self.data_loader = None
        self.query_executor = None

    def run(self):
        try:
            # Step 1: Extract data from the distributed system
            self.data_extractor = DataExtractor()
            data = self.data_extractor.extract_data(self.input_path)

            # Step 2: Transform the data
            file_name = self.input_path.split('/')[-1].split('.')[0]
            self.data_transformer = DataTransformer()
            transformed_data = self.data_transformer.transform_data(data, self.date, file_name)

            # Step 3: Load the transformed data into the database
            self.data_loader = DataLoader(DB_SETTINGS)
            self.data_loader.process_stage(transformed_data, self.output_table, file_name, DB_SETTINGS)

            # Step 4: Execute SQL queries on the loaded data
            self.query_executor = QueryExecutor(DB_SETTINGS)
            self.query_executor.execute_queries()

        finally:
            # Step 5: Clean up resources and connections
            if self.data_extractor:
                self.data_extractor.close()

            if self.data_transformer:
                self.data_transformer.close()

            if self.data_loader:
                self.data_loader.close()

            if self.query_executor:
                self.query_executor.close()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Data Engineering Assessment")
    parser.add_argument("--date", help="Date of processing the file", default='2023-07-14')
    parser.add_argument("--input", help="Input JSON data path",
                        default='FilesToProcess/2023/Orders_2023-07-01_to_2023-07-31.json')
    parser.add_argument("--output", help="Output stage table name", default='customer_orders')
    args = parser.parse_args()

    # Create an instance of the MainWorkflow class
    workflow = MainWorkflow(args.date, args.input, args.output)

    # Run the main workflow
    workflow.run()
