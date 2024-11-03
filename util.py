import logging
from sqlalchemy import create_engine
from psycopg2 import sql

def setup_logging(log_file: str):
    """Sets up logging to log to both a file and the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),  # Log to file
            logging.StreamHandler()         # Log to console
        ]
    )



def check_and_create_partition(country: str, db_uri: str, table_name: str, schema: str):
    """
    Checks if a partition table exists for the specified country and creates it if it doesn't.

    Parameters:
    - country (str): The name of the country for which to check/create the partition.
    - db_uri (str): Database URI to connect to the PostgreSQL instance.
    - table_name (str): Name of the main table to which partitions belong.
    - schema (str): Schema in which the table and partitions are located (default is 'public').

    Returns:
    - bool: True if partition exists or was created successfully, False if there was an error.
    """
    try:
        # Create a database connection
        engine = create_engine(db_uri)
        conn = engine.raw_connection()
        cur = conn.cursor()

        # Clean up the country name (remove extra spaces, etc.)
        country = country.strip()

        # Check if the partition already exists
        partition_name = f"{table_name}_{country.lower()}"
        cur.execute(f"SELECT to_regclass('{schema}.{partition_name}')")
        partition_exists = cur.fetchone()[0]

        if partition_exists:
            logging.info(f"Partition for country '{country}' already exists.")
            return True

        # Create the partition if it doesn't exist
        create_partition_query = sql.SQL('''
            CREATE TABLE {schema}.{partition_name} PARTITION OF {schema}.{table_name}
            FOR VALUES IN (%s);
        ''').format(
            schema=sql.Identifier(schema),
            partition_name=sql.Identifier(partition_name),
            table_name=sql.Identifier(table_name)
        )
        
        cur.execute(create_partition_query, [country])
        conn.commit()
        logging.info(f"Partition created for country: {country}")
        return True

    except Exception as e:
        logging.error(f"Error checking or creating partition for country '{country}': {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()
