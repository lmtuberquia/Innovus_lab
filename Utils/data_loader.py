from Tools.database_utilities import *

class DataLoader:
    def __init__(self, db_config):
        """
                Initializes the DataLoader object.

                Args:
                    db_config (dict): Configuration parameters for connecting to the database.
                        Example:
                        {
                            "host": "myhost",
                            "port": 0000,
                            "database": "mydatabase",
                            "user": "myuser",
                            "password": "mypassword"
                        }
        """
        self.connection = connect_to_database(db_config)

    def process_stage(self, dataframe, stage_table, file_name, db_config):
        """
                Processes and loads a DataFrame into a stage table in the database.

                Args:
                    dataframe (pyspark.sql.DataFrame): The DataFrame to be loaded.
                    stage_table (str): The name of the stage table in the database.
                    file_name (str): The name of the file being processed.
                    db_config (dict): Configuration parameters for connecting to the database.

                Raises:
                    Exception: If there is an error loading the data into the table.
        """
        try:
            if check_table_existence(self.connection, stage_table, 'stage'):
                logging.info('table exist '+stage_table)
            truncate_table(self.connection, stage_table, 'stage')

            load_dataframe(self.connection, dataframe, stage_table, 'stage', db_config)
            logging.info("Data loaded into table: %s", stage_table)
            log_operation(self.connection, file_name, stage_table, 'records')

        except Exception as e:
            logging.error("Error loading data into table: %s", e)

    def close(self):
        """
                Closes the connection to the database.
        """
        self.connection.close()
        logging.info("Connection to MySQL database closed")