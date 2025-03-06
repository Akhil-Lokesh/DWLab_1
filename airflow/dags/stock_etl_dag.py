from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import logging
from utils import get_stock_data, load_to_snowflake, STOCK_SYMBOLS

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Table name in Snowflake
STOCK_TABLE = 'STOCK_PRICES'

# DAG definition
dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='Extract stock data from yfinance and load to Snowflake',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'etl'],
)

def extract_and_load(symbol, **kwargs):
    """Extract stock data and load it to Snowflake"""
    try:
        logging.info(f"Extracting data for {symbol}")
        df = get_stock_data(symbol)
        logging.info(f"Extracted {len(df)} records for {symbol}")
        
        logging.info(f"Loading data for {symbol} to Snowflake")
        load_to_snowflake(df, STOCK_TABLE)
        
        return f"Successfully processed {len(df)} records for {symbol}"
    
    except Exception as e:
        logging.error(f"Error processing {symbol}: {str(e)}")
        raise

# Create tasks for each stock symbol
for symbol in STOCK_SYMBOLS:
    task = PythonOperator(
        task_id=f'extract_load_{symbol}',
        python_callable=extract_and_load,
        op_kwargs={'symbol': symbol},
        dag=dag,
    )

if __name__ == "__main__":
    dag.cli()