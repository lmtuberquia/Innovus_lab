import logging
import mysql.connector
from pyspark.sql import DataFrame


def connect_to_database(db_config):
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"]
        )
        return connection
        logging.info("Connected to the MySQL database")
        return connection
    except mysql.connector.Error as error:
        logging.error("Failed to connect to the MySQL database: %s", error)


def check_table_existence(connection, schema, table_name):
    try:
        cursor = connection.cursor()
        query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
        cursor.execute(query, (schema, table_name))
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        logging.error("Error checking table existence: %s", str(e))
        return False

def load_dataframe(connection, dataframe: DataFrame, table_name, schema, db_config):
    try:
        url = "jdbc:mysql://{host}:{port}/{database}".format(
            host=db_config["host"],
            port=db_config["port"],
            database=schema
        )

        cursor = connection.cursor()
        cursor.execute("USE `{}`".format(schema))

        dataframe.write.format("jdbc").options(
            url=url,
            driver=db_config["driver"],
            dbtable=table_name,
            user=db_config["user"],
            password=db_config["password"]
        ).mode("append").save()

        logging.info("Data loaded into table: %s", table_name)

    except Exception as e:
        logging.error("Error loading data into table: %s", str(e))


def truncate_table(connection, table_name, schema):
    try:
        cursor = connection.cursor()
        cursor.execute("USE `{}`".format(schema))

        truncate_query = "TRUNCATE TABLE `{}`".format(table_name)
        cursor.execute(truncate_query)
        connection.commit()

        logging.info("Table %s truncated", table_name)

    except Exception as e:
        logging.error("Error truncating table: %s", str(e))


def log_operation(connection, file_name, stage_table, schema):
    try:
        cursor = connection.cursor()
        cursor.execute("USE `{}`".format(schema))

        log_query = "INSERT INTO `customer_load_transactions` (loadDate, fileName, stageTable) VALUES (NOW(), %s, %s)"
        cursor.execute(log_query, (file_name, stage_table))
        connection.commit()

        logging.info("Operation logged for file: %s, stage table: %s", file_name, stage_table)

    except Exception as e:
        logging.error("Error logging operation: %s", str(e))


def execute_stored_procedure(connection, sp_name, schema):
    try:
        cursor = connection.cursor()
        cursor.execute("USE `{}`".format(schema))

        cursor.execute("CALL {}".format(sp_name))
        if cursor.description is not None:  # Check if there is a result set
            result = cursor.fetchone()
            logging.info("Stored procedure executed successfully: %s", sp_name)
            return result
        else:
            logging.info("Stored procedure executed successfully: %s", sp_name)
            return None

        logging.info("Stored procedure executed successfully: %s", sp_name)
        return result

    except Exception as e:
        logging.error("Error executing stored procedure: %s", str(e))
