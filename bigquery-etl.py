import os
import json
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config():
    credentials_file = os.getenv(
        'DB_CREDENTIALS_FILE', 
        '/mnt/c/Users/Carlota/Documents/coding projects/etl_bigquery/db_credentials.json'
    )
    try:
        with open(credentials_file, 'r') as f:
            credentials = json.load(f)
    except FileNotFoundError:
        logger.error(f"Credentials file not found at {credentials_file}.")
        raise
    except json.JSONDecodeError:
        logger.error("Error decoding the JSON credentials file.")
        raise

    return {
        "user": credentials['username'],
        "password": credentials['password'],
        "host": credentials['host'],
        "database": credentials['database'],
        "proj": 'valued-vault-439309-a6',
        "dataset": 'sample_dataset',
        "target_table": 'annual_movie_summary'
    }


def extract_data(query, engine):
    try:
        logger.info("Extracting data from MySQL...")
        df = pd.read_sql(query, engine)
        if df.empty:
            raise ValueError("Query returned no data.")
        logger.info("Data extraction successful.")
        return df
    except Exception as e:
        logger.error("Error during data extraction.")
        raise e


def transform_data(df):
    logger.info("Transforming data...")
    conditions = [
        df['avg_rating'] <= 4,
        df['avg_rating'] <= 6,
        df['avg_rating'] <= 10,
    ]
    choices = ['bad movie year', 'okay movie year', 'good movie year']
    df['year_rating'] = np.select(conditions, choices, default='not rated')
    logger.info("Data transformation complete.")
    return df


def load_data_to_bq(df, table_id, client):
    """Load transformed data into BigQuery."""
    try:
        logger.info("Loading data to BigQuery...")
        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"Loaded {client.get_table(table_id).num_rows} rows to {table_id}")
    except Exception as e:
        logger.error("Error during data loading.")
        raise e


def main():
 
    try:
        # Load configurations
        config = load_config()
        engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}/{config['database']}")
        client = bigquery.Client(project=config['proj'])
        table_id = f"{config['proj']}.{config['dataset']}.{config['target_table']}"

        # Define SQL query
        query = '''
            SELECT year, 
                   COUNT(imdb_title_id) AS movie_count, 
                   AVG(duration) AS avg_movie_duration, 
                   AVG(avg_vote) AS avg_rating
            FROM `u479841347_sql_course`.`imdb_movies`
            GROUP BY year
        '''

        # ETL steps
        df = extract_data(query, engine)
        df = transform_data(df)
        load_data_to_bq(df, table_id, client)

    except Exception as e:
        logger.error(f"ETL process failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
