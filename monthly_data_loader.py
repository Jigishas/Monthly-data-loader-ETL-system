import os
import sys
import logging
import datetime
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# Configure logging to output to stdout with INFO level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Directory to save monthly CSV data files
DATA_SAVE_PATH = os.getenv("DATA_SAVE_PATH", "./monthly_data")
# File to store timestamp of last successful run
LAST_RUN_FILE = os.path.join(DATA_SAVE_PATH, "last_run.txt")

def get_snowflake_credentials():
    """
    Retrieve Snowflake credentials securely from environment variables.
    Exits the program if any required credential is missing.
    Returns a dictionary of credentials.
    """
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "JOSADMIN")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if not user or not password or not account:
        logger.error("Snowflake credentials are not fully set in environment variables")
        sys.exit(1)
    return {
        "user": user,
        "password": password,
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "role": role
    }

def load_data_to_snowflake(pandas_df, table_name, sf_creds):
    """
    Load a pandas DataFrame into a Snowflake table.
    Creates the table if it does not exist.
    """
    ctx = None
    cs = None
    try:
        # Connect to Snowflake using provided credentials
        ctx = snowflake.connector.connect(
            user=sf_creds["user"],
            password=sf_creds["password"],
            account=sf_creds["account"],
            warehouse=sf_creds["warehouse"],
            database=sf_creds["database"],
            schema=sf_creds["schema"],
            role=sf_creds["role"]
        )
        cs = ctx.cursor()
        # Create table if not exists with example schema
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING,
            data STRING,
            timestamp TIMESTAMP
        )
        """
        cs.execute(create_table_sql)
        # Use Snowflake's write_pandas utility to load data
        success, nchunks, nrows, _ = write_pandas(ctx, pandas_df, table_name)
        if success:
            logger.info(f"Loaded {nrows} rows into Snowflake table {table_name}")
        else:
            logger.error("Failed to load data into Snowflake")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        sys.exit(1)
    finally:
        # Clean up cursor and connection
        if cs:
            cs.close()
        if ctx:
            ctx.close()

def fetch_public_data():
    """
    Placeholder function to fetch data from a public streaming source.
    Replace this with actual data extraction logic.
    For demonstration, generates a simple DataFrame with random data.
    """
    import random
    now = datetime.datetime.utcnow()
    data = {
        "id": [str(i) for i in range(10)],
        "data": [f"value_{random.randint(1,100)}" for _ in range(10)],
        "timestamp": [now for _ in range(10)]
    }
    df = pd.DataFrame(data)
    return df

def read_last_run_time():
    """
    Reads the timestamp of the last successful run from a file.
    Returns a datetime object or None if file does not exist or is invalid.
    """
    if not os.path.exists(LAST_RUN_FILE):
        return None
    try:
        with open(LAST_RUN_FILE, "r") as f:
            timestamp_str = f.read().strip()
            return datetime.datetime.fromisoformat(timestamp_str)
    except Exception as e:
        logger.warning(f"Failed to read last run time: {e}")
        return None

def write_last_run_time(run_time):
    """
    Writes the timestamp of the current run to a file.
    Creates the directory if it does not exist.
    """
    os.makedirs(DATA_SAVE_PATH, exist_ok=True)
    with open(LAST_RUN_FILE, "w") as f:
        f.write(run_time.isoformat())

def should_run_monthly():
    """
    Determines if the monthly data extraction should run.
    Returns True if no previous run or if 30 or more days have passed since last run.
    """
    last_run = read_last_run_time()
    if last_run is None:
        return True
    now = datetime.datetime.utcnow()
    delta = now - last_run
    return delta.days >= 30

def save_data_monthly():
    """
    Main function to perform monthly data extraction, saving, and loading.
    Checks if monthly run is due, fetches data, saves as CSV, loads into Snowflake,
    and updates last run timestamp.
    """
    if not should_run_monthly():
        logger.info("Monthly data load not required yet.")
        return
    logger.info("Starting monthly data extraction and load.")
    df = fetch_public_data()
    os.makedirs(DATA_SAVE_PATH, exist_ok=True)
    file_name = f"data_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    file_path = os.path.join(DATA_SAVE_PATH, file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Saved data to {file_path}")

    sf_creds = get_snowflake_credentials()
    load_data_to_snowflake(df, "MONTHLY_PUBLIC_DATA", sf_creds)
    write_last_run_time(datetime.datetime.utcnow())
    logger.info("Monthly data extraction and load completed.")

if __name__ == "__main__":
    save_data_monthly()
