import os
import json
import logging
from typing import Dict, Any
import psycopg2
import psycopg2.extras
from logging.handlers import RotatingFileHandler
from datetime import datetime
from utils.db_helpers import get_db_connection
from pathlib import Path

###############################################################################
# Logging Configuration
###############################################################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("db_ingest.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# Resolve Paths Dynamically
###############################################################################
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Directory of this script
CONFIG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "configs"))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))


def normalize_path(path: str) -> str:
    """Convert a file path to a consistent format with forward slashes."""
    return Path(path).as_posix()


###############################################################################
# Database Helpers
###############################################################################

def get_db_config(config_path: str) -> Dict[str, Any]:
    """Load database connection details from a JSON config."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Database config not found: {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def ensure_tables_exist(conn: psycopg2.extensions.connection) -> None:
    """
    Create or update the 'stocks' and 'stock_prices' tables if they do not exist,
    and ensure a unique index on (stock_id, trade_date) for 'stock_prices'.
    """
    create_stocks_sql = """
    CREATE TABLE IF NOT EXISTS stocks (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(50) NOT NULL UNIQUE,
        company_name VARCHAR(255),
        industry VARCHAR(255),
        isin VARCHAR(20),
        meta_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """

    create_stock_prices_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        stock_id INT NOT NULL REFERENCES stocks(id),
        symbol VARCHAR(50) NOT NULL,
        trade_date DATE NOT NULL,
        high_price NUMERIC,
        low_price NUMERIC,
        open_price NUMERIC,
        close_price NUMERIC,
        last_traded_price NUMERIC,
        previous_close_price NUMERIC,
        total_traded_qty BIGINT,
        total_traded_value NUMERIC,
        high_52week NUMERIC,
        low_52week NUMERIC,
        total_trades INT,
        delivery_qty BIGINT,
        delivery_perc NUMERIC,
        vwap NUMERIC,
        meta_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """

    create_unique_idx_sql = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_prices_unique
    ON stock_prices(stock_id, trade_date);
    """

    with conn.cursor() as cur:
        cur.execute(create_stocks_sql)
        cur.execute(create_stock_prices_sql)
        cur.execute(create_unique_idx_sql)
    conn.commit()
    logger.info("Ensured 'stocks' and 'stock_prices' tables and indexes exist.")

###############################################################################
# Insert / Upsert Logic
###############################################################################

def upsert_stock(conn: psycopg2.extensions.connection, stock_item: Dict[str, Any]) -> int:
    """
    Insert or update (ON CONFLICT) a stock in the 'stocks' table based on 'symbol'.
    If 'symbol' exists, fields are updated. Otherwise, a new row is created.

    Returns: The 'id' (primary key) of the row in 'stocks'.
    """
    data = dict(stock_item)
    symbol = data.pop("symbol", None)
    company_name = data.pop("meta_companyName", None)
    industry = data.pop("meta_industry", None)
    isin = data.pop("meta_isin", None)

    meta_data_json = psycopg2.extras.Json(data, dumps=json.dumps)

    with conn.cursor() as cur:
        sql = """
        INSERT INTO stocks (
            symbol, company_name, industry, isin, meta_data
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            industry = EXCLUDED.industry,
            isin = EXCLUDED.isin,
            meta_data = EXCLUDED.meta_data,
            updated_at = NOW()
        RETURNING id;
        """
        cur.execute(sql, (symbol, company_name, industry, isin, meta_data_json))
        stock_id = cur.fetchone()[0]
    conn.commit()
    return stock_id

def upsert_stock_price(
    conn: psycopg2.extensions.connection,
    stock_id: int,
    price_item: Dict[str, Any]
) -> int:
    """
    Insert or update (ON CONFLICT) a single price row into 'stock_prices' based on (stock_id, trade_date).
    This ensures if the same date is inserted again, we update instead of inserting a duplicate.

    Returns: The new or updated row's 'id'.
    """
    data = dict(price_item)

    # Symbol can be stored in prices for convenience; might come from CH_SYMBOL
    symbol = data.pop("CH_SYMBOL", None)

    # Convert date
    trade_date_str = data.pop("CH_TIMESTAMP", None)
    trade_date = None
    if trade_date_str:
        try:
            trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("Invalid CH_TIMESTAMP '%s' for stock_id=%d", trade_date_str, stock_id)

    def sanitize_numeric(value, default=None):
        """Convert value to integer or return default."""
        if value is None:
            return default
        if isinstance(value, int):  # Already an integer
            return value
        if isinstance(value, str) and value.isdigit():  # String containing a valid integer
            return int(value)
        return default


    def sanitize_float(value, default=None):
        """Convert value to float or return default."""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    # Sanitize numeric fields
    high_price = sanitize_float(data.pop("CH_TRADE_HIGH_PRICE", None))
    low_price = sanitize_float(data.pop("CH_TRADE_LOW_PRICE", None))
    open_price = sanitize_float(data.pop("CH_OPENING_PRICE", None))
    close_price = sanitize_float(data.pop("CH_CLOSING_PRICE", None))
    last_traded_price = sanitize_float(data.pop("CH_LAST_TRADED_PRICE", None))
    previous_close_price = sanitize_float(data.pop("CH_PREVIOUS_CLS_PRICE", None))
    total_traded_qty = sanitize_numeric(data.pop("CH_TOT_TRADED_QTY", None))
    total_traded_value = sanitize_float(data.pop("CH_TOT_TRADED_VAL", None))
    high_52week = sanitize_float(data.pop("CH_52WEEK_HIGH_PRICE", None))
    low_52week = sanitize_float(data.pop("CH_52WEEK_LOW_PRICE", None))
    total_trades = sanitize_numeric(data.pop("CH_TOTAL_TRADES", None))
    delivery_qty = sanitize_numeric(data.pop("COP_DELIV_QTY", None))
    delivery_perc = sanitize_float(data.pop("COP_DELIV_PERC", None))
    vwap = sanitize_float(data.pop("VWAP", None))

    meta_data_json = psycopg2.extras.Json(data, dumps=json.dumps)

    with conn.cursor() as cur:
        sql = """
        INSERT INTO stock_prices (
            stock_id, symbol, trade_date, high_price, low_price, open_price, close_price,
            last_traded_price, previous_close_price, total_traded_qty, total_traded_value,
            high_52week, low_52week, total_trades, delivery_qty, delivery_perc, vwap, meta_data
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id, trade_date)
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            open_price = EXCLUDED.open_price,
            close_price = EXCLUDED.close_price,
            last_traded_price = EXCLUDED.last_traded_price,
            previous_close_price = EXCLUDED.previous_close_price,
            total_traded_qty = EXCLUDED.total_traded_qty,
            total_traded_value = EXCLUDED.total_traded_value,
            high_52week = EXCLUDED.high_52week,
            low_52week = EXCLUDED.low_52week,
            total_trades = EXCLUDED.total_trades,
            delivery_qty = EXCLUDED.delivery_qty,
            delivery_perc = EXCLUDED.delivery_perc,
            vwap = EXCLUDED.vwap,
            meta_data = EXCLUDED.meta_data,
            updated_at = NOW()
        RETURNING id;
        """
        cur.execute(sql, (
            stock_id, symbol, trade_date, high_price, low_price, open_price, close_price,
            last_traded_price, previous_close_price, total_traded_qty, total_traded_value,
            high_52week, low_52week, total_trades, delivery_qty, delivery_perc, vwap,
            meta_data_json
        ))
        row_id = cur.fetchone()[0]
    conn.commit()
    return row_id

###############################################################################
# Example: Ingesting Stocks and Prices
###############################################################################

def ingest_stocks(conn: psycopg2.extensions.connection, transformed_file: str) -> None:
    """
    Reads 'transformed_stock_list.json' (flattened) and upserts each stock with 'priority = 0' into 'stocks'.
    """
    if not os.path.exists(transformed_file):
        logger.error("Transformed file not found at %s", transformed_file)
        return

    with open(transformed_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        records = data.get("data", []) if isinstance(data, dict) else data

    # Filter records with priority = 0
    priority_zero_records = [record for record in records if record.get("priority", -1) == 0]
    logger.info("Found %d records with priority=0 from %s", len(priority_zero_records), transformed_file)

    for item in priority_zero_records:
        stock_id = upsert_stock(conn, item)
        logger.debug("Upserted stock symbol=%s => id=%d", item.get("symbol"), stock_id)
    logger.info("Done ingesting stocks into DB from %s", transformed_file)

def ingest_prices_for_symbol(
    conn: psycopg2.extensions.connection,
    stock_id: int,
    symbol: str,
    prices_file: str
) -> None:
    """
    Reads the 'prices_file' JSON for a given symbol, upserting each record into 'stock_prices'.
    """
    prices_file = normalize_path(prices_file)  # Normalize path for logging
    if not os.path.exists(prices_file):
        logger.warning(f"No price file found for symbol={symbol} at path={prices_file}")
        return

    with open(prices_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        elif isinstance(data, dict):
            data = [data]

    logger.info(f"Ingesting {len(data)} price rows for symbol={symbol} (stock_id={stock_id}) from file={prices_file}.")
    for price_item in data:
        _pid = upsert_stock_price(conn, stock_id, price_item)
    logger.info(f"Done ingesting price data for {symbol} from file={prices_file}.")


def ingest_all_prices(conn: psycopg2.extensions.connection, config: Dict[str, Any]) -> None:
    """
    For every symbol in the DB, attempt to load that symbol's prices JSON file
    using the path template in config["output_paths"]["stock_prices"] and upsert them.
    """
    base_dir = os.path.abspath(os.path.dirname(__file__))  # Get the directory of the script
    prices_template = os.path.join(base_dir, "..", config["output_paths"]["stock_prices"])

    with conn.cursor() as cur:
        cur.execute("SELECT id, symbol FROM stocks")
        rows = cur.fetchall()

    for stock_id, symbol in rows:
        prices_file = prices_template.format(symbol=symbol.upper()).lower()
        if not os.path.exists(prices_file):
            logger.warning("No price file found for symbol=%s at path=%s", symbol, prices_file)
            continue

        ingest_prices_for_symbol(conn, stock_id, symbol, prices_file)

###############################################################################
# Main Entry Point
###############################################################################

def main() -> None:
    """
    1) Reads db_config to connect to Postgres.
    2) Ensures 'stocks' and 'stock_prices' tables exist, with unique index for (stock_id, trade_date).
    3) Upserts data from 'transformed_stock_list.json' into 'stocks' (for priority = 0).
    4) Optionally loads each symbol's price file from config paths and upserts them into 'stock_prices'.
     """
    logger.info("Starting DB ingestion process...")

    # 1) Load DB config
    db_config_path = os.path.join(CONFIG_DIR, "db_config.json")
    db_config = get_db_config(db_config_path)

    # 2) Connect & ensure tables exist
    conn = get_db_connection(db_config)
    ensure_tables_exist(conn)

    # 3) Ingest the transformed stock list
    transformed_file = os.path.join(DATA_DIR, "indices", "transformed_stock_list.json")
    ingest_stocks(conn, transformed_file)

    # 4) Ingest all prices
    config_path = os.path.join(BASE_DIR, "..", "config.json")
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        ingest_all_prices(conn, config)

    conn.close()


if __name__ == "__main__":
    main()
