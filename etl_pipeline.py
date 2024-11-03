import pandas as pd
import logging
from sqlalchemy import create_engine, text
from util import setup_logging, check_and_create_partition
from io import StringIO

class ETLPipeline:
    def extract(self, source: str) -> pd.DataFrame:
        """Extracts data from a pipe-delimited text file where H denotes headers and D denotes data rows."""
        logging.info(f'Extracting data from {source}')
        try:
            # Read the text file into a list of lines
            with open(source, 'r') as file:
                lines = file.readlines()
            
            # Extract header and data rows
            header_line = None
            data_rows = []
            
            for line in lines:
                if line.startswith("H|"):
                    header_line = line.strip().split('|')[1:]  # Exclude the 'H|' prefix
                elif line.startswith("D|"):
                    data_rows.append(line.strip().split('|')[1:])  # Exclude the 'D|' prefix
            
            # Check if header exists
            if not header_line:
                raise ValueError("No header found in file")
            
            # Create DataFrame
            data = pd.DataFrame(data_rows, columns=header_line)
            data.to_csv('trans/data.csv', index = False)
            logging.info(f'Number of records extracted: {len(data)}')
            return data
        
        except Exception as e:
            logging.error(f"Failed to load data from {source}: {e}")
            raise


    def load_stg(self, data: pd.DataFrame, table_name: str, db_uri: str, schema: str):
        """Loads the extracted data into a PostgreSQL staging table with schema."""
        logging.info(f'Loading data into table {schema}.{table_name}')
        try:
            engine = create_engine(db_uri)
            conn = engine.raw_connection()
            cur = conn.cursor()

            # Create a list of unique countries in the data
            data['Country'] = data['Country'].str.strip()  # Strip whitespace if needed
            unique_countries = data['Country'].str.strip()

            # Iterate through each country and call check_and_create_partition
            for country in unique_countries:
                check_and_create_partition(country, db_uri, table_name, schema)
            
            # Iterate through each country and call check_and_create_partition
            for country in unique_countries:
                check_and_create_partition(country, db_uri, table_name, 'curate')

            # Remove white spaces from country column
            # Use StringIO to write data into an in-memory buffer
            buffer = StringIO()
            data.to_csv(buffer, index=False, header=False)
            buffer.seek(0)  # Move to the beginning of the buffer

            # Open a connection and create a cursor
            with conn.cursor() as cur:
                # Delete existing records from the staging table
                cur.execute(f"DELETE FROM {schema}.{table_name}")
                conn.commit()  # Commit the delete operation
                
                # Use the COPY command with StringIO buffer
                cur.copy_expert(f"""
                    COPY {schema}.{table_name} (
                        Customer_Name, 
                        Customer_ID, 
                        Customer_Open_Date, 
                        Last_Consulted_Date, 
                        Vaccination_Type, 
                        Doctor_Consulted, 
                        State, 
                        Country, 
                        Date_of_Birth, 
                        Active_Customer
                    ) FROM STDIN WITH CSV
                    """, buffer)
                
                conn.commit()  # Commit the COPY operation

            
            logging.info(f"Data loaded into table {schema}.{table_name} successfully.")
        except KeyError as e:
            logging.error(f"Column not found during loading: {e}")
            raise
        except Exception as e:
            logging.error(f"Error during loading: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    def incremental_load(self, db_uri: str, schema: str, table_name: str):
        """Handles the incremental load into country-specific tables."""
        logging.info(f'Performing incremental load for curate.{table_name}')
        try:
            engine = create_engine(db_uri)
            with engine.connect() as conn:
                # Incremental load SQL
                sql = f"""
                WITH Customer_Updates AS (
                    SELECT 
                        Customer_Name,
                        Customer_ID,
                        Customer_Open_Date,
                        Last_Consulted_Date,
                        Vaccination_Type,
                        Doctor_Consulted,
                        State,
                        Country,
                        Post_Code,
                        Date_of_Birth,
                        Active_Customer,
                        EXTRACT(YEAR FROM AGE(Date_of_Birth)) AS Age,
                        CURRENT_DATE - Last_Consulted_Date AS Days_since_last_consulted,
                        ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY Last_Consulted_Date DESC) AS rn
                    FROM {schema}.{table_name}
                    WHERE CURRENT_DATE - Last_Consulted_Date > 30
                )
                INSERT INTO curate.{table_name} (
                    Customer_Name, 
                    Customer_ID, 
                    Customer_Open_Date, 
                    Last_Consulted_Date, 
                    Vaccination_Type, 
                    Doctor_Consulted, 
                    State, 
                    Country, 
                    Post_Code, 
                    Date_of_Birth, 
                    Active_Customer, 
                    Age, 
                    Days_since_last_consulted
                )
                SELECT 
                    Customer_Name,
                    Customer_ID,
                    Customer_Open_Date,
                    Last_Consulted_Date,
                    Vaccination_Type,
                    Doctor_Consulted,
                    State,
                    Country,
                    Post_Code,
                    Date_of_Birth,
                    Active_Customer,
                    Age,
                    Days_since_last_consulted
                FROM Customer_Updates
                WHERE rn = 1
                ON CONFLICT (Customer_ID, Country) DO UPDATE 
                SET 
                    Last_Consulted_Date = EXCLUDED.Last_Consulted_Date,
                    Age = EXCLUDED.Age,
                    Days_since_last_consulted = EXCLUDED.Days_since_last_consulted;
                """
                # Execute the SQL command
                print(sql)
                conn.execute(text(sql))
            logging.info("Incremental load completed successfully.")
        except Exception as e:
            logging.error(f"Error during incremental loading: {e}")
            raise


# Example of running the ETL process
if __name__ == "__main__":
    log_file_path = r"C:\Users\acer\OneDrive\Desktop\Incubyte\data_pipeline\logs\etl_pipeline.log"
    setup_logging(log_file_path)

    db_uri = 'postgresql+psycopg2://postgres:root@localhost:5432/incubyte'

    pipeline = ETLPipeline()
    extracted_data = pipeline.extract(r"C:\Users\acer\OneDrive\Desktop\Incubyte\data_pipeline\source\dummy_data.txt")
    
    pipeline.load_stg(extracted_data, 'customer_info', db_uri, 'stg')
    
    # Perform incremental load
    pipeline.incremental_load(db_uri, 'stg', 'customer_info')