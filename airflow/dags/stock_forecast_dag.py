from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from utils import (
    run_forecasting_query, 
    create_final_table, 
    STOCK_SYMBOLS
)

# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Table names
SOURCE_TABLE = 'STOCK_PRICES'
FORECAST_TABLE = 'STOCK_FORECASTS'
FINAL_TABLE = 'STOCK_ANALYSIS'

# DAG definition
dag = DAG(
    'stock_forecast_pipeline',
    default_args=default_args,
    description='Forecast stock prices using Snowflake ML',
    schedule_interval='30 0 * * *',  # Run daily at 00:30 (after ETL pipeline)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'forecast', 'ml'],
)

def forecast_stock(symbol, **kwargs):
    """Run forecasting for a specific stock"""
    try:
        logging.info(f"Forecasting prices for {symbol}")
        run_forecasting_query(symbol, SOURCE_TABLE, FORECAST_TABLE)
        return f"Successfully forecasted prices for {symbol}"
    
    except Exception as e:
        logging.error(f"Error forecasting {symbol}: {str(e)}")
        raise

def create_combined_table(**kwargs):
    """Create the final combined table with historical and forecast data"""
    try:
        logging.info("Creating final combined table")
        create_final_table(SOURCE_TABLE, FORECAST_TABLE, FINAL_TABLE)
        return "Successfully created final table"
    
    except Exception as e:
        logging.error(f"Error creating final table: {str(e)}")
        raise

# Create forecasting tasks for each stock symbol
forecast_tasks = []
for symbol in STOCK_SYMBOLS:
    task = PythonOperator(
        task_id=f'forecast_{symbol}',
        python_callable=forecast_stock,
        op_kwargs={'symbol': symbol},
        dag=dag,
    )
    forecast_tasks.append(task)

# Create task to combine data into final table
combine_task = PythonOperator(
    task_id='create_final_table',
    python_callable=create_combined_table,
    dag=dag,
)

# Set dependencies - run all forecasting tasks before combining
for task in forecast_tasks:
    task >> combine_task

if __name__ == "__main__":
    dag.cli()