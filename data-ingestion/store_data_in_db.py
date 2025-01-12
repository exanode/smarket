import psycopg2
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Validate stock list
def validate_stock_list(file_path):
    """Check if the stock list JSON file contains the required headers."""
    try:
        with open(file_path, 'r') as f:
            stock_list = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Stock list file not found: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in file: {file_path}")

    logging.info(f"Validating stock list file: {file_path}")
    required_keys = ['symbol', 'identifier']

    if 'data' not in stock_list or not stock_list['data']:
        raise ValueError(f"Invalid JSON structure: 'data' key is missing or empty in {file_path}")

    first_record = stock_list['data'][0]
    missing_keys = [key for key in required_keys if key not in first_record]

    if missing_keys:
        raise ValueError(f"Missing keys in stock list file {file_path}: {missing_keys}")

    logging.info(f"Stock list file {file_path} passed validation.")
    return stock_list['data']

# Validate stock prices
def validate_stock_prices(file_path):
    """Check if the stock prices JSON file contains the required headers."""
    try:
        with open(file_path, 'r') as f:
            stock_prices = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Stock prices file not found: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in file: {file_path}")

    logging.info(f"Validating stock prices file: {file_path}")
    required_keys = ['CH_SYMBOL', 'CH_TIMESTAMP', 'CH_OPENING_PRICE', 'CH_CLOSING_PRICE']

    if 'data' not in stock_prices or not stock_prices['data']:
        raise ValueError(f"Invalid JSON structure: 'data' key is missing or empty in {file_path}")

    first_record = stock_prices['data'][0]
    missing_keys = [key for key in required_keys if key not in first_record]

    if missing_keys:
        raise ValueError(f"Missing keys in stock prices file {file_path}: {missing_keys}")

    logging.info(f"Stock prices file {file_path} passed validation.")
    return stock_prices['data']

# Insert validated stock list into database
def insert_validated_stock_list(file_path, db_config):
    """Validate and insert stock list into the database."""
    valid_data = validate_stock_list(file_path)

    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        for stock in valid_data:
            cursor.execute("""
                INSERT INTO Stocks (symbol, identifier, companyName, industry, isin, priority)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING;
            """, (stock['symbol'], stock['identifier'], stock['meta_companyName'], stock['meta_industry'], stock['meta_isin'], stock['priority']))
        connection.commit()
        logging.info("Validated stock list inserted into the database.")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# Insert validated stock prices into database
def insert_validated_stock_prices(file_path, db_config):
    """Validate and insert stock prices into the database."""
    valid_data = validate_stock_prices(file_path)

    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        for record in valid_data:
            cursor.execute("""
                INSERT INTO HistoricalPrices (
                    symbol, date, openingPrice, closingPrice, highPrice, lowPrice, lastTradedPrice,
                    previousClosePrice, totalTradedQty, totalTradedValue, totalTrades, deliveryQty,
                    deliveryPercentage, vwap, weekHighPrice, weekLowPrice
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                record['CH_SYMBOL'], record['CH_TIMESTAMP'], record['CH_OPENING_PRICE'], record['CH_CLOSING_PRICE'],
                record['CH_TRADE_HIGH_PRICE'], record['CH_TRADE_LOW_PRICE'], record['CH_LAST_TRADED_PRICE'],
                record['CH_PREVIOUS_CLS_PRICE'], record['CH_TOT_TRADED_QTY'], record['CH_TOT_TRADED_VAL'],
                record['CH_TOTAL_TRADES'], record.get('COP_DELIV_QTY', 0), record.get('COP_DELIV_PERC', 0.0),
                record.get('VWAP', 0.0), record['CH_52WEEK_HIGH_PRICE'], record['CH_52WEEK_LOW_PRICE']
            ))
        connection.commit()
        logging.info("Validated stock prices inserted into the database.")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# Main execution for validation and insertion
if __name__ == "__main__":
    config_path = "config.json"
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        db_config = config["db"]
        stock_list_file = config["output_paths"]["stock_list"]
        stock_price_files = config["output_paths"]["stock_prices"]

        # Insert stock list
        insert_validated_stock_list(stock_list_file, db_config)

        # Insert stock prices
        for stock_price_file in stock_price_files:
            insert_validated_stock_prices(stock_price_file, db_config)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
