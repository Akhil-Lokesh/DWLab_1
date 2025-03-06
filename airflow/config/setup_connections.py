"""
This script creates the required Airflow connections and variables when the container starts.
You should run this once after the Airflow container is up.
"""

from airflow import settings
from airflow.models import Connection, Variable
import json

# Define Snowflake connection
snowflake_conn = Connection(
    conn_id='snowflake_conn',
    conn_type='snowflake',
    host='<your-snowflake-account>.snowflakecomputing.com',  # Replace with your account
    login='<your-username>',  # Replace with your username
    password='<your-password>',  # Replace with your password
    schema='PUBLIC',
    extra=json.dumps({
        'account': '<your-account-identifier>',  # Replace with your account identifier (without .snowflakecomputing.com)
        'warehouse': 'COMPUTE_WH',  # Replace with your warehouse name
        'database': 'STOCK_DB',     # Replace with your database name
        'schema': 'PUBLIC',         # Replace with your schema name
        'role': 'ACCOUNTADMIN',     # Replace with your role
        'authenticator': 'snowflake',
    })
)

# Add connections to Airflow DB
session = settings.Session()

# Check if connection already exists
existing_conn = session.query(Connection).filter(Connection.conn_id == snowflake_conn.conn_id).first()
if existing_conn:
    session.delete(existing_conn)
    session.add(snowflake_conn)
    print(f"Updated connection: {snowflake_conn.conn_id}")
else:
    session.add(snowflake_conn)
    print(f"Created connection: {snowflake_conn.conn_id}")

# Set up Alpha Vantage API key as a Variable
Variable.set(
    key="alpha_vantage_api_key",
    value="<your-alpha-vantage-api-key>",  # Replace with your Alpha Vantage API key
    description="API key for Alpha Vantage"
)
print("Created Variable: alpha_vantage_api_key")

session.commit()
session.close()
print("Setup complete!")