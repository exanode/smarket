"""
This script fetches a list of stocks for a specified index (e.g., 'NIFTY 100')
and retrieves historical or recent stock prices based on a configuration file.

Expected config.json structure:
{
    "index_name": "NIFTY 100",
    "output_paths": {
        "stock_list": "data/indices/{index_name}_stock_list.json",
        "stock_prices": "data/stock_prices/{symbol}_historical_prices.json",
        "stock_names": "data/stock_names.json"
    },
    "price_fetch_settings": {
        "from_date": "",
        "to_date": ""
    }
}

Usage:
    python main.py
    (Optionally pass test_mode=True to the main function for testing)
"""

import os
import json
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from logging.handlers import RotatingFileHandler

# ----------------------------------------------------------------------------------
# Logging Configuration
# ----------------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------------------

def parse_date(date_str: str) -> Optional[datetime]:
    """Convert a date string in 'd-m-y' format to a datetime object.

    If date_str is empty, returns None.

    Args:
        date_str: The date string in 'd-m-y' format (e.g., "01-01-2023").

    Returns:
        A datetime object if valid, else None.

    Raises:
        ValueError: If the date string is not in the expected format 'd-m-y'.
    """
    if not date_str:
        return None  # If empty, we choose not to parse
    try:
        return datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError as e:
        logger.error("Invalid date format '%s'. Expected 'd-m-y'.", date_str)
        raise e


def format_date(date_obj: datetime) -> str:
    """Format a datetime object as a string in 'd-m-y' format.

    Args:
        date_obj: The datetime object to format.

    Returns:
        The formatted date string in 'd-m-y' format (e.g., "01-01-2023").
    """
    return date_obj.strftime("%d-%m-%Y")

# ----------------------------------------------------------------------------------
# Core Functions
# ----------------------------------------------------------------------------------

def load_config(file_path: str) -> Dict[str, Any]:
    """Load and return configuration from a JSON file.

    Args:
        file_path: Path to the JSON configuration file.

    Returns:
        A dictionary containing the configuration parameters.

    Raises:
        FileNotFoundError: If the config file does not exist.
        json.JSONDecodeError: If the file contents are not valid JSON.
    """
    logger.info("Loading configuration from %s", file_path)

    if not os.path.exists(file_path):
        logger.error("Configuration file not found: %s", file_path)
        raise FileNotFoundError(f"Config file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_fetch_stock_list(index_name: str, output_path: str) -> None:
    """Call an external script to fetch the stock list for the given index.

    Args:
        index_name: Name of the stock index (e.g., 'NIFTY 100').
        output_path: Path where the resulting JSON file should be stored.

    Raises:
        subprocess.CalledProcessError: If the external script fails.
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
    """Call an external script to fetch stock prices and append results to output_path.

    - The script stores data to a temporary file.
    - Merges existing data in output_path with new data, deduplicates, and writes back.

    Args:
        symbol: Stock symbol (e.g., 'AAPL').
        from_date: Start date in 'd-m-y' format.
        to_date: End date in 'd-m-y' format.
        output_path: Destination file path for merged stock price data.

    Raises:
        subprocess.CalledProcessError: If the external script fails.
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

    # Load existing data
    existing_data = []
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as existing_file:
            try:
                existing_data = json.load(existing_file)
                if isinstance(existing_data, dict):
                    # Convert dict to list if needed
                    if "data" in existing_data:
                        existing_data = existing_data["data"]
                    else:
                        existing_data = [existing_data]
            except json.JSONDecodeError:
                logger.warning("Existing file has invalid JSON: %s", output_path)

    # Load newly fetched data
    with open(temp_output_path, 'r', encoding='utf-8') as temp_file:
        new_data = json.load(temp_file)
        if isinstance(new_data, dict):
            # Convert dict to list if needed
            if "data" in new_data:
                new_data = new_data["data"]
            else:
                new_data = [new_data]

    # Merge and deduplicate
    combined_data = existing_data + new_data
    combined_data = list({json.dumps(entry, sort_keys=True): entry for entry in combined_data}.values())

    # Write back to output_path
    with open(output_path, 'w', encoding='utf-8') as output_file:
        json.dump(combined_data, output_file, indent=4)

    # Remove the temporary file
    os.remove(temp_output_path)
    logger.info("Updated stock prices saved to %s", output_path)


def fetch_stock_prices_by_dates(symbol: str, start_date: datetime, end_date: datetime, output_path: str) -> None:
    """Fetch stock prices in yearly chunks from start_date to end_date.

    Args:
        symbol: Stock symbol (e.g., 'TCS').
        start_date: Start datetime.
        end_date: End datetime.
        output_path: File path for accumulated results.
    """
    logger.info("Fetching historical prices for %s from %s to %s in chunks.",
                symbol, format_date(start_date), format_date(end_date))

    current_date = start_date
    one_year = timedelta(days=365)

    while current_date < end_date:
        chunk_end_date = min(current_date + one_year, end_date)
        run_fetch_stock_prices(
            symbol,
            format_date(current_date),
            format_date(chunk_end_date),
            output_path
        )
        current_date = chunk_end_date + timedelta(days=1)

# ----------------------------------------------------------------------------------
# Step Implementations
# ----------------------------------------------------------------------------------

def fetch_stock_list_step(config: Dict[str, Any]) -> str:
    """Fetch the stock list for the configured index and return the output path.

    Args:
        config: Configuration dictionary with keys:
                - 'index_name': Name of the index (e.g., 'NIFTY 100')
                - 'output_paths': A dict containing 'stock_list' path templates.

    Returns:
        The actual file path where the stock list is stored.
    """
    index_name = config["index_name"]
    stock_list_template = config["output_paths"]["stock_list"]
    output_path = stock_list_template.format(index_name=index_name.replace(" ", "_"))

    # Fetch the stock list using an external script
    run_fetch_stock_list(index_name, output_path)

    return output_path


def fetch_stock_prices_step(stock_names: List[str], config: Dict[str, Any]) -> None:
    """Fetch prices for each symbol in stock_names based on config.

    - Determines from/to dates from config or uses defaults.
    - If the file for a symbol already exists, fetches recent prices (yesterday/today).
      Otherwise, fetches historical data back to from_date.

    Args:
        stock_names: List of stock symbols.
        config: Configuration dictionary with keys:
                - 'output_paths': Paths for output files.
                - 'price_fetch_settings': Contains optional 'from_date' and 'to_date'.
    """
    logger.info("Beginning stock price fetch for %d symbols...", len(stock_names))

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    from_date_str = config["price_fetch_settings"].get("from_date", "")
    to_date_str = config["price_fetch_settings"].get("to_date", "")

    from_date = parse_date(from_date_str) or datetime(2015, 1, 1)
    to_date = parse_date(to_date_str) or today

    for symbol in stock_names:
        stock_prices_template = config["output_paths"]["stock_prices"]
        output_path = stock_prices_template.format(symbol=symbol.upper()).lower()

        if os.path.exists(output_path):
            logger.info("Fetching recent data for %s (yesterday/today).", symbol)
            run_fetch_stock_prices(
                symbol,
                format_date(yesterday),
                format_date(today),
                output_path
            )
        else:
            logger.info("Fetching historical data for %s from %s to %s.",
                        symbol, format_date(from_date), format_date(to_date))
            fetch_stock_prices_by_dates(symbol, from_date, to_date, output_path)

# ----------------------------------------------------------------------------------
# Main Entry Point
# ----------------------------------------------------------------------------------

def main(test_mode: bool = False) -> None:
    """Main script to orchestrate fetching stock list and prices.

    Steps:
      1. Load configuration from 'config.json'.
      2. Fetch the stock list (and optionally stop if test_mode = True).
      3. Extract symbol names from the stock list and save to a file.
      4. Fetch stock prices (historical or recent) for each symbol.

    Args:
        test_mode: If True, only fetches the stock list and then exits.
    """
    logger.info("Starting stock data fetch workflow...")

    # 1. Load Configuration
    config = load_config("config.json")

    # 2. Fetch Stock List
    stock_list_path = fetch_stock_list_step(config)
    if test_mode:
        logger.warning("Test mode enabled. Exiting after stock list fetch.")
        return

    # 3. Extract and Save Stock Symbols
    logger.info("Extracting stock symbols from %s...", stock_list_path)
    with open(stock_list_path, 'r', encoding='utf-8') as f:
        stock_list_content = json.load(f)

    stock_names = [
        item["symbol"]
        for item in stock_list_content.get("data", [])
        if item.get("priority", 0) == 0
    ]

    stock_names_path = config["output_paths"]["stock_names"]
    with open(stock_names_path, 'w', encoding='utf-8') as f:
        json.dump(stock_names, f, indent=4)
    logger.info("Saved %d stock names to %s", len(stock_names), stock_names_path)

    # 4. Fetch Stock Prices
    fetch_stock_prices_step(stock_names, config)

    logger.info("Workflow completed successfully.")


if __name__ == "__main__":
    main(test_mode=False)
