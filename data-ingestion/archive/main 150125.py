import os
import json
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from logging.handlers import RotatingFileHandler

###############################################################################
# Logging Configuration
###############################################################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("app.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# Date Parsing & Formatting
###############################################################################

def parse_date(date_str: str) -> Optional[datetime]:
    """
    Convert a string in 'd-m-y' format into a datetime object.
    
    Example:
        parse_date("01-01-2023") -> datetime(2023, 1, 1)
    
    Args:
        date_str: A date string in 'd-m-y' format (e.g. '01-01-2023').
    
    Returns:
        A datetime object if successfully parsed, otherwise None if empty.
    
    Raises:
        ValueError: If 'date_str' does not match '%d-%m-%Y' format.
    """
    if not date_str:
        return None
    return datetime.strptime(date_str, "%d-%m-%Y")


def format_date(date_obj: datetime) -> str:
    """
    Format a datetime object as a string in 'd-m-y' format.
    
    Example:
        datetime(2023, 1, 1) -> '01-01-2023'
    
    Args:
        date_obj: A datetime object to format.
    
    Returns:
        A string representing the date in 'd-m-y' format.
    """
    return date_obj.strftime("%d-%m-%Y")

###############################################################################
# Configuration
###############################################################################

def load_config(file_path: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        file_path: The path to the configuration JSON file.
    
    Returns:
        A dictionary containing configuration parameters.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contents are invalid JSON.
    """
    logger.info("Loading configuration from %s", file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

###############################################################################
# External Script Calls
###############################################################################

def run_fetch_stock_list(index_name: str, output_path: str) -> None:
    """
    Execute an external script to fetch the stock list for the given index.
    
    Args:
        index_name: The name of the stock index (e.g., 'NIFTY 100').
        output_path: Path to the JSON file where the list will be saved.
    
    Raises:
        subprocess.CalledProcessError: If the script exits with a non-zero status.
    """
    logger.info("Fetching stock list for index: %s", index_name)
    logger.info("Saving stock list to: %s", output_path)

    command = [
        "python",
        "scripts/fetch_stock_list.py",
        "--index_name", index_name,
        "--output", output_path
    ]
    subprocess.run(command, check=True)


def run_fetch_stock_prices(symbol: str, from_date: str, to_date: str, output_path: str) -> None:
    """
    Execute an external script to fetch stock prices for a given symbol in a specified date range.
    New data is temporarily stored, then merged with existing data (if any) in 'output_path'.
    
    Args:
        symbol: Stock symbol (e.g., 'TCS').
        from_date: Start date in 'd-m-y' format (e.g., '01-01-2023').
        to_date: End date in 'd-m-y' format (e.g., '31-01-2023').
        output_path: JSON file path for merging old and new data.
    
    Raises:
        subprocess.CalledProcessError: If the script exits with a non-zero status.
    """
    logger.info("Fetching prices for %s from %s to %s", symbol, from_date, to_date)
    temp_output_path = f"{output_path}.tmp"

    command = [
        "python",
        "scripts/fetch_stock_prices.py",
        "--symbol", symbol,
        "--output", temp_output_path
    ]
    if from_date:
        command += ["--from_date", from_date]
    if to_date:
        command += ["--to_date", to_date]

    subprocess.run(command, check=True)

    # Merge new data with existing, deduplicating entries
    existing_data = []
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as existing_file:
            try:
                existing_data = json.load(existing_file)
                if isinstance(existing_data, dict):
                    if "data" in existing_data:
                        existing_data = existing_data["data"]
                    else:
                        existing_data = [existing_data]
            except json.JSONDecodeError:
                logger.warning("Existing file has invalid JSON: %s", output_path)

    with open(temp_output_path, 'r', encoding='utf-8') as temp_file:
        new_data = json.load(temp_file)
        if isinstance(new_data, dict):
            if "data" in new_data:
                new_data = new_data["data"]
            else:
                new_data = [new_data]

    combined_data = existing_data + new_data
    # Remove duplicates by JSON-serializing each item with sort_keys
    combined_data = list({json.dumps(entry, sort_keys=True): entry for entry in combined_data}.values())

    with open(output_path, 'w', encoding='utf-8') as output_file:
        json.dump(combined_data, output_file, indent=4)

    os.remove(temp_output_path)
    logger.info("Updated stock prices saved to %s", output_path)


def run_transform_stock_list(input_file: str, output_file: str) -> None:
    """
    Execute an external script (transform_stock_list.py) to flatten/transform the stock list data.
    
    Args:
        input_file: The original JSON file path (e.g., 'data/indices/NIFTY_100_stock_list.json').
        output_file: The transformed JSON file path (e.g., 'data/indices/transformed_stock_list.json').
    
    Raises:
        subprocess.CalledProcessError: If the script exits with a non-zero status.
    """
    logger.info("Running transform_stock_list.py on %s => %s", input_file, output_file)
    command = [
        "python",
        "scripts/transform_stock_list.py",
        "--input", input_file,
        "--output", output_file
    ]
    subprocess.run(command, check=True)


def run_populate_stock_metadata(config_file: str, metadata_file: str) -> None:
    """
    Execute an external script (populate_stock_metadata.py) to build/update symbol metadata
    based on the newly transformed data and available stock prices.
    
    Args:
        config_file: The path to the configuration file (e.g., 'config.json').
        metadata_file: The path to the symbol metadata file (e.g., 'symbol_metadata.json').
    
    Raises:
        subprocess.CalledProcessError: If the script exits with a non-zero status.
    """
    logger.info("Populating/updating symbol metadata using populate_stock_metadata.py")
    command = [
        "python",
        "scripts/populate_stock_metadata.py",
        "--config", config_file,
        "--metadata_file", metadata_file
    ]
    subprocess.run(command, check=True)

###############################################################################
# Metadata (Read-Only)
###############################################################################

def load_symbol_metadata(metadata_file_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Loads symbol metadata from a JSON file, returning a dict keyed by symbol.
    
    Example metadata JSON:
        [
          {
            "symbol": "TCS",
            "listing_date": "2015-01-01",
            "start_date": "2015-01-01",
            "end_date": "2025-01-13"
          },
          ...
        ]
    
    Returns a structure like:
        {
          "TCS": { "symbol": "TCS", "end_date": "2025-01-13", ... },
          ...
        }
    
    Args:
        metadata_file_path: The path to 'symbol_metadata.json'.
    
    Returns:
        A dictionary keyed by symbol, or an empty dict if file is missing or invalid.
    """
    if not os.path.exists(metadata_file_path):
        logger.warning("Metadata file %s not found. Returning empty dict.", metadata_file_path)
        return {}

    try:
        with open(metadata_file_path, "r", encoding="utf-8") as f:
            metadata_list = json.load(f)
    except json.JSONDecodeError:
        logger.error("Invalid JSON in metadata file %s. Returning empty dict.", metadata_file_path)
        return {}

    result = {}
    for entry in metadata_list:
        sym = entry.get("symbol")
        if sym:
            result[sym] = entry
    return result

###############################################################################
# Chunked Fetch
###############################################################################

def fetch_stock_prices_by_dates(symbol: str, start_date: datetime, end_date: datetime, output_path: str) -> None:
    """
    Fetch stock prices in yearly chunks (365 days) from start_date to end_date.
    Each chunk calls run_fetch_stock_prices, which merges newly fetched data 
    into the existing file (if any).
    
    Args:
        symbol: The stock symbol (e.g., 'TCS').
        start_date: datetime object representing the earliest date to fetch.
        end_date: datetime object representing the latest date to fetch.
        output_path: Path where combined price data will be stored.
    """
    logger.info("Fetching historical prices for %s from %s to %s in 1-year chunks.",
                symbol, format_date(start_date), format_date(end_date))

    current_date = start_date
    one_year = timedelta(days=365)

    while current_date <= end_date:
        chunk_end_date = min(current_date + one_year, end_date)
        run_fetch_stock_prices(
            symbol,
            format_date(current_date),
            format_date(chunk_end_date),
            output_path
        )
        current_date = chunk_end_date + timedelta(days=1)

###############################################################################
# Main Steps
###############################################################################

def fetch_stock_list_step(config: Dict[str, Any]) -> str:
    """
    Fetch the stock list for the configured index and return the path to the fetched JSON file.
    
    Args:
        config: The loaded configuration dict containing 'index_name' and 'output_paths'.
    
    Returns:
        The path to the newly fetched stock list JSON file.
    """
    index_name = config["index_name"]
    stock_list_template = config["output_paths"]["stock_list"]
    output_path = stock_list_template.format(index_name=index_name.replace(" ", "_"))

    run_fetch_stock_list(index_name, output_path)
    return output_path


def fetch_stock_prices_step(stock_names: List[str], config: Dict[str, Any]) -> None:
    """
    For each symbol in 'stock_names', determines a start_date by reading 'symbol_metadata.json' 
    (if it exists), then fetches data up to a configured 'to_date' or today's date.
    
    Fetch is done in yearly chunks to avoid large date-range calls. Old data is merged with new.
    
    Args:
        stock_names: List of stock symbols to fetch.
        config: Configuration dict with 'price_fetch_settings' for date bounds, 
                and 'output_paths' for file templates.
    """
    logger.info("Beginning stock price fetch for %d symbols...", len(stock_names))

    today = datetime.now()
    from_date_str = config["price_fetch_settings"].get("from_date", "")
    to_date_str = config["price_fetch_settings"].get("to_date", "")

    config_from_date = parse_date(from_date_str) or datetime(2015, 1, 1)
    config_to_date = parse_date(to_date_str) or today

    # Load existing symbol metadata (read-only)
    metadata_file_path = "symbol_metadata.json"
    metadata_dict = load_symbol_metadata(metadata_file_path)

    for symbol in stock_names:
        stock_prices_template = config["output_paths"]["stock_prices"]
        output_path = stock_prices_template.format(symbol=symbol.upper()).lower()

        # If symbol is in metadata, resume from end_date+1
        md_entry = metadata_dict.get(symbol, {})
        last_known_end_str = md_entry.get("end_date", "")
        if last_known_end_str:
            try:
                last_end_dt = datetime.strptime(last_known_end_str, "%Y-%m-%d")
                start_date = last_end_dt + timedelta(days=1)
            except ValueError:
                logger.warning("Invalid end_date '%s' in metadata for %s. Using default start date.",
                               last_known_end_str, symbol)
                start_date = config_from_date
        else:
            start_date = config_from_date

        end_date = min(config_to_date, today)
        if start_date > end_date:
            logger.info("No new data to fetch for %s; start_date=%s, end_date=%s",
                        symbol, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            continue

        # Fetch data in 1-year chunks
        fetch_stock_prices_by_dates(symbol, start_date, end_date, output_path)

    logger.info("Done fetching prices for all symbols.")

###############################################################################
# Main Entry Point
###############################################################################

def main() -> None:
    """
    Main data ingestion pipeline for stock data:
    
      1. Load 'config.json'.
      2. Fetch the stock list for the configured index (e.g., 'NIFTY 100').
      3. Extract symbol names with 'priority=0', save them to a file (e.g., 'stock_names.json').
      4. For each symbol, fetch prices (resuming from metadata end_date if found), 
         merging new data with existing data files.
      5. Run 'transform_stock_list.py' to flatten the fetched stock list (if needed).
      6. Run 'populate_stock_metadata.py' to update/create metadata based on newly fetched data.
    """
    logger.info("Starting stock data fetch workflow.")
    config_path = "config.json"
    config = load_config(config_path)

    # Step 1: Fetch stock list
    stock_list_path = fetch_stock_list_step(config)

    # Step 2: Extract & save stock symbols
    logger.info("Extracting symbols from %s", stock_list_path)
    with open(stock_list_path, 'r', encoding='utf-8') as f:
        stock_list_data = json.load(f)

    stock_names = [
        item["symbol"]
        for item in stock_list_data.get("data", [])
        if item.get("priority", 0) == 0
    ]

    stock_names_path = config["output_paths"]["stock_names"]
    with open(stock_names_path, 'w', encoding='utf-8') as f:
        json.dump(stock_names, f, indent=4)
    logger.info("Saved %d stock names to %s", len(stock_names), stock_names_path)

    # Step 3: Fetch stock prices (resume from metadata if found)
    fetch_stock_prices_step(stock_names, config)

    # Step 4: Transform the fetched stock list (e.g., flatten meta fields)
    transformed_stock_list_path = "data/indices/transformed_stock_list.json"
    run_transform_stock_list(
        input_file=stock_list_path,
        output_file=transformed_stock_list_path
    )

    # Step 5: Populate or update symbol metadata
    metadata_file_path = "symbol_metadata.json"
    run_populate_stock_metadata(
        config_file=config_path,
        metadata_file=metadata_file_path
    )

    logger.info("Data ingestion pipeline completed successfully.")


if __name__ == "__main__":
    main()
