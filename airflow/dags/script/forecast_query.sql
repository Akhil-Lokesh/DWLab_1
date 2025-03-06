-- This SQL uses Snowflake's Time Series functions to forecast stock prices
-- Create a temporary table to store the forecasts
CREATE OR REPLACE TEMPORARY TABLE temp_forecasts AS
WITH stock_data AS (
    SELECT 
        stock_symbol,
        date,
        close
    FROM 
        stock_historical_data
    WHERE 
        date >= DATEADD(DAY, -180, CURRENT_DATE())
),
forecast_data AS (
    SELECT 
        stock_symbol,
        date,
        close,
        -- Generate forecasts using Snowflake's time series functions
        -- The FORECAST function predicts future values based on historical data
        FORECAST_LINEAR(close) OVER (
            PARTITION BY stock_symbol 
            ORDER BY date 
            ROWS BETWEEN 180 PRECEDING AND 7 FOLLOWING
        ) AS predicted_close,
        -- Calculate prediction intervals for confidence bounds
        FORECAST_LINEAR_LOWER(close, 80) OVER (
            PARTITION BY stock_symbol 
            ORDER BY date 
            ROWS BETWEEN 180 PRECEDING AND 7 FOLLOWING
        ) AS prediction_interval_lower,
        FORECAST_LINEAR_UPPER(close, 80) OVER (
            PARTITION BY stock_symbol 
            ORDER BY date 
            ROWS BETWEEN 180 PRECEDING AND 7 FOLLOWING
        ) AS prediction_interval_upper
    FROM 
        stock_data
)
SELECT 
    stock_symbol,
    DATEADD(DAY, seq, CURRENT_DATE()) AS date,
    predicted_close,
    prediction_interval_lower,
    prediction_interval_upper
FROM 
    forecast_data,
    TABLE(GENERATOR(ROWCOUNT => 7)) -- Generate 7 days of forecasts
WHERE 
    DATEADD(DAY, seq, CURRENT_DATE()) > CURRENT_DATE() -- Only future dates
ORDER BY 
    stock_symbol, 
    date;

-- Insert the forecasts into the forecast table
-- DELETE existing forecasts for the same date range to avoid duplicates
DELETE FROM stock_forecast_data 
WHERE date BETWEEN CURRENT_DATE() AND DATEADD(DAY, 7, CURRENT_DATE());

-- Insert new forecasts
INSERT INTO stock_forecast_data (
    stock_symbol,
    date,
    predicted_close,
    prediction_interval_lower,
    prediction_interval_upper
)
SELECT 
    stock_symbol,
    date,
    predicted_close,
    prediction_interval_lower,
    prediction_interval_upper
FROM 
    temp_forecasts;

-- Generate a unified view that combines historical and forecast data
-- This first empties the unified table
TRUNCATE TABLE stock_unified_data;

-- Insert historical data
INSERT INTO stock_unified_data
SELECT 
    'historical' as data_type,
    stock_symbol,
    date,
    close as value,
    NULL as prediction_interval_lower,
    NULL as prediction_interval_upper
FROM 
    stock_historical_data
WHERE 
    date >= DATEADD(DAY, -30, CURRENT_DATE());

-- Insert forecast data
INSERT INTO stock_unified_data
SELECT 
    'forecast' as data_type,
    stock_symbol,
    date,
    predicted_close as value,
    prediction_interval_lower,
    prediction_interval_upper
FROM 
    stock_forecast_data
WHERE 
    date > CURRENT_DATE();

-- Query to check the results
SELECT * FROM stock_unified_data
ORDER BY stock_symbol, date;