from Tools.database_utilities import *

class QueryExecutor:
    def __init__(self, db_config):
        """
                Initializes the QueryExecutor object.

                Args:
                    db_config (dict): Configuration parameters for connecting to the database.
                        Example:
                        {
                            "host": "localhost",
                            "port": 3306,
                            "database": "mydatabase",
                            "user": "myuser",
                            "password": "mypassword"
                        }
        """
        self.connection = connect_to_database(db_config)

    def execute_queries(self):

        """
                Executes the predefined stored procedures.

                Raises:
                    Exception: If there is an error executing any of the stored procedures.
        """
        try:
            execute_stored_procedure(self.connection, "load_dim_customer", "dimension")
            execute_stored_procedure(self.connection, "load_dim_countries", "dimension")
            execute_stored_procedure(self.connection, "load_fact_orders", "fact")
            execute_stored_procedure(self.connection, "create_orders_principal_view", "widget")
            execute_stored_procedure(self.connection, "create_customer_ordersummary_view", "widget")
            execute_stored_procedure(self.connection, "create_customer_5_ordersummary_view", "widget")
            execute_stored_procedure(self.connection, "create_recent_60_customer_orders_view", "widget")
            execute_stored_procedure(self.connection, "create_monthly_order_views", "widget")

        except Exception as e:
            logging.error("Error executing stored procedures: %s", e)

        # Close the database connection
    def close(self):
        """
                Closes the connection to the database.
        """
        self.connection.close()
        logging.info("Connection to MySQL database closed")