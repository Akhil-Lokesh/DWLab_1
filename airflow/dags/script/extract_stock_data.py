import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

def extract_stock_data(stock_symbol, days=180):
    """
    Extract historical stock data for a given symbol for the last X days
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"Fetching data for {stock_symbol} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Get stock data
        stock_data = yf.download(
            stock_symbol,
            start=start_date,
            end=end_date,
            progress=False
        )
        
        # Reset index to make date a column
        stock_data = stock_data.reset_index()
        
        # Rename columns to match our Snowflake table
        stock_data = stock_data.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Add stock symbol column
        stock_data['stock_symbol'] = stock_symbol
        
        # Select only the columns we need
        stock_data = stock_data[['stock_symbol', 'date', 'open', 'close', 'low', 'high', 'volume']]
        
        logger.info(f"Successfully fetched {len(stock_data)} rows for {stock_symbol}")
        
        return stock_data
    
    except Exception as e:
        logger.error(f"Error fetching data for {stock_symbol}: {str(e)}")
        raise

def load_to_snowflake(conn_params, df):
    """
    Load dataframe to Snowflake table
    """
    try:
        # Adjust the connection parameters based on authentication type
        connect_params = {
            'user': conn_params['user'],
            'account': conn_params['account'],
            'warehouse': conn_params['warehouse'],
            'database': conn_params['database'],
            'schema': conn_params['schema']
        }
        
        # Add authenticator if using external browser
        if 'authenticator' in conn_params and conn_params['authenticator'] == 'externalbrowser':
            connect_params['authenticator'] = 'externalbrowser'
        else:
            # Only add password if not using external browser
            connect_params['password'] = conn_params.get('password', '')
        
        conn = snowflake.connector.connect(**connect_params)
        
        cursor = conn.cursor()
        
        # Begin transaction
        cursor.execute("BEGIN")
        
        try:
            # Write the dataframe to Snowflake
            success, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df,
                table_name="STOCK_HISTORICAL_DATA",
                schema=conn_params['schema'],
                database=conn_params['database']
            )
            
            # Commit if successful
            if success:
                cursor.execute("COMMIT")
                logger.info(f"Successfully loaded {num_rows} rows to Snowflake")
            else:
                cursor.execute("ROLLBACK")
                logger.error("Failed to load data to Snowflake")
                
        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Error during Snowflake data load: {str(e)}")
            raise
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def main(stock_symbols, conn_params):
    """
    Main function to extract stock data and load to Snowflake
    """
    all_data = pd.DataFrame()
    
    # Get data for each stock
    for symbol in stock_symbols:
        try:
            df = extract_stock_data(symbol.strip())
            all_data = pd.concat([all_data, df])
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {str(e)}")
    
    # If we have data, load it to Snowflake
    if not all_data.empty:
        load_to_snowflake(conn_params, all_data)
    else:
        logger.warning("No data was collected. Nothing to load to Snowflake.")

if __name__ == "__main__":
    # This can be used for testing the script directly
    stock_symbols = ["AAPL", "NVDA"]
    
    conn_params = {
        'user': 'AKHILOKESH',
        'account': 'MBPXQSI-DLB82834',
        'warehouse': 'STOCK_ANALYTICS_WH',
        'database': 'STOCK_PREDICTION',
        'schema': 'STOCK_DATA',
        'authenticator': 'externalbrowser'
    }
    
    main(stock_symbols, conn_params)