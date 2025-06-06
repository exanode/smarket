"""
This script creates or updates a JSON file (e.g., 'symbol_metadata.json') with the following structure
for each symbol:
    {
        "symbol": <STRING>,
        "listing_date": <STRING in 'YYYY-MM-DD' format>,
        "start_date": <STRING in 'YYYY-MM-DD' format>,
        "end_date": <STRING in 'YYYY-MM-DD' format>
    }

1) It reads the main config.json to locate:
    - The index file path for the specified index (to retrieve listing_date).
    - The folder or pattern for stock_prices JSON files (to retrieve min/max dates).
2) For each symbol found in the index file, it:
    - Ensures the entry has "priority" == 0 (otherwise skip).
    - Extracts 'symbol' and 'listingDate' (now from `meta_listingDate`).
    - Loads the corresponding `stock_prices` JSON to find min/max date (earliest & latest).
      (If file does not exist, or there's no data, defaults apply.)
3) It merges this information into the 'symbol_metadata.json':
    - If a symbol is missing, creates a new entry.
    - If a symbol exists, updates listing_date / start_date / end_date as needed.

Usage Example:
    python populate_stock_metadata.py --config config.json --metadata_file symbol_metadata.json
"""

import os
import json
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from logging.handlers import RotatingFileHandler

# --------------------------------------------------------------------------------
# Logging Configuration
# --------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("populate_stock_metadata.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------
DATE_FMT = "%Y-%m-%d"
DEFAULT_EARLIEST_DATE = "2015-01-01"

# --------------------------------------------------------------------------------
# JSON Utilities
# --------------------------------------------------------------------------------

def load_json_file(file_path: str) -> Any:
    """
    Load and return JSON data from the specified file.
    If the file doesn't exist, return None.
    """
    if not os.path.exists(file_path):
        logger.warning("File not found: %s", file_path)
        return None
    logger.info("Loading JSON from %s", file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json_file(data: Any, file_path: str) -> None:
    """
    Write the provided data to the specified JSON file.
    Ensures the directory path exists.
    """
    directory = os.path.dirname(file_path)
    if directory:  # Only create directories if a directory path is specified
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Saving JSON to %s", file_path)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


# --------------------------------------------------------------------------------
# Metadata Helpers
# --------------------------------------------------------------------------------

def update_or_create_symbol_entry(
    symbol: str,
    listing_date_str: str,
    earliest_stock_date: str,
    latest_stock_date: str,
    metadata: List[Dict[str, Any]]
) -> None:
    """
    Updates or creates an entry for a stock symbol in the metadata.

    This function ensures the symbol's metadata includes accurate 
    listing date, start date, and end date. If no stock data is available, 
    'end_date' remains unset to allow future data fetching.

    Args:
        symbol (str): The stock symbol.
        listing_date_str (str): The listing date of the stock in 'YYYY-MM-DD' format.
        earliest_stock_date (str): The earliest date from the fetched stock data.
        latest_stock_date (str): The latest date from the fetched stock data.
        metadata (List[Dict[str, Any]]): The metadata list to update or append entries.

    Returns:
        None: Modifies the 'metadata' list in place.

    Raises:
        None: Logs warnings for invalid dates but does not raise exceptions.
    """
    logger.info("Updating/Creating metadata entry for symbol: %s", symbol)

    # Helper to find an existing metadata entry
    def find_symbol_in_metadata(symbol: str, entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for entry in entries:
            if entry.get("symbol") == symbol:
                return entry
        return None

    existing_entry = find_symbol_in_metadata(symbol, metadata)

    try:
        listing_date_dt = datetime.strptime(listing_date_str, DATE_FMT)
    except (ValueError, TypeError):
        listing_date_dt = datetime.strptime(DEFAULT_EARLIEST_DATE, DATE_FMT)
        logger.warning("Invalid listing date for %s. Defaulting to %s.", symbol, DEFAULT_EARLIEST_DATE)

    earliest_listed_dt = max(
        listing_date_dt, 
        datetime.strptime(DEFAULT_EARLIEST_DATE, DATE_FMT)
    )

    try:
        earliest_dt = datetime.strptime(earliest_stock_date, DATE_FMT) if earliest_stock_date else None
    except ValueError:
        logger.warning("Invalid earliest stock date for %s. Ignoring.", symbol)
        earliest_dt = None

    try:
        latest_dt = datetime.strptime(latest_stock_date, DATE_FMT) if latest_stock_date else None
    except ValueError:
        logger.warning("Invalid latest stock date for %s. Ignoring.", symbol)
        latest_dt = None

    potential_starts = [earliest_listed_dt]
    if earliest_dt:
        potential_starts.append(earliest_dt)

    start_date_dt = min(potential_starts)

    if latest_dt:
        end_date_dt = latest_dt
    else:
        logger.info("No valid latest stock date for %s. Leaving end_date unset.", symbol)
        end_date_dt = None

    if end_date_dt and listing_date_dt > end_date_dt:
        end_date_dt = listing_date_dt

    final_listing_date = listing_date_dt.strftime(DATE_FMT)
    final_start_date = start_date_dt.strftime(DATE_FMT)
    final_end_date = end_date_dt.strftime(DATE_FMT) if end_date_dt else ""

    if not existing_entry:
        metadata.append({
            "symbol": symbol,
            "listing_date": final_listing_date,
            "start_date": final_start_date,
            "end_date": final_end_date
        })
        logger.info("Created new metadata entry for %s.", symbol)
    else:
        existing_entry["listing_date"] = final_listing_date
        existing_entry["start_date"] = final_start_date
        existing_entry["end_date"] = final_end_date
        logger.info("Updated metadata entry for %s.", symbol)



def extract_symbol_dates_from_prices(prices_path: str) -> Tuple[str, str]:
    """
    Given a path to a JSON file containing stock prices data (assumed to be a list of objects),
    find the min and max date in 'YYYY-MM-DD' format from the "CH_TIMESTAMP" field.

    Example entry in your JSON:
    [
        {
            "CH_TIMESTAMP": "2025-01-08",
            "open": 12.3,
            "close": 12.7,
            ...
        },
        ...
    ]

    Return:
        Tuple[str, str]: (earliest_date_str, latest_date_str)
        or ("", "") if no data or file not found or invalid format.
    """
    logger.debug("Extracting min/max date from prices at: %s", prices_path)
    data = load_json_file(prices_path)
    if not data or not isinstance(data, list):
        logger.warning("No valid list data found in %s", prices_path)
        return "", ""

    date_strings = []
    for item in data:
        date_val = item.get("CH_TIMESTAMP")
        if date_val:
            date_strings.append(date_val)

    if not date_strings:
        logger.warning("No 'CH_TIMESTAMP' fields found in %s", prices_path)
        return "", ""

    valid_dates = []
    for d in date_strings:
        try:
            valid_dates.append(datetime.strptime(d, DATE_FMT))
        except ValueError:
            logger.debug("Skipping invalid CH_TIMESTAMP '%s' in %s", d, prices_path)

    if not valid_dates:
        logger.warning("No valid dates after parsing in %s", prices_path)
        return "", ""

    earliest = min(valid_dates).strftime(DATE_FMT)
    latest = max(valid_dates).strftime(DATE_FMT)
    logger.debug("Earliest: %s, Latest: %s in %s", earliest, latest, prices_path)
    return earliest, latest



# --------------------------------------------------------------------------------
# Main Logic
# --------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Populate or update a symbol_metadata.json with listing_date, start_date, and end_date."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to the config.json file. Default: 'config.json'."
    )
    parser.add_argument(
        "--metadata_file",
        default="symbol_metadata.json",
        help="Path to the symbol_metadata.json file to create/update. Default: 'symbol_metadata.json'."
    )
    args = parser.parse_args()

    # 1) Load config.json to find paths
    if not os.path.exists(args.config):
        logger.error("Config file not found at: %s", args.config)
        return
    with open(args.config, 'r', encoding='utf-8') as cf:
        config = json.load(cf)
    logger.info("Loaded config from %s", args.config)

    index_name = config["index_name"]
    stock_list_path = config["output_paths"]["transformed_stock_list"].format(index_name=index_name).replace(" ","_")
    prices_template = config["output_paths"]["stock_prices"]  # e.g. "data/stock_prices/{symbol}_historical_prices.json"

    # 2) Load the index stock list JSON
    if not os.path.exists(stock_list_path):
        logger.error("Stock list file not found at %s", stock_list_path)
        return
    logger.info("Loading index data from %s", stock_list_path)
    with open(stock_list_path, 'r', encoding='utf-8') as f:
        index_data = json.load(f)

    # 3) Prepare to update metadata
    metadata_file = args.metadata_file
    existing_metadata = load_json_file(metadata_file)
    if not existing_metadata or not isinstance(existing_metadata, list):
        logger.warning("Metadata file %s not a list or doesn't exist. Initializing as empty list.", metadata_file)
        existing_metadata = []

    # 4) For each symbol in the index, ensure:
    #    - priority == 0
    #    - listing_date from item["meta_listingDate"]
    #    Then read stock_prices to find min/max date, update metadata.
    logger.info("Processing index data to build/update metadata entries...")
    for item in index_data.get("data", []):
        # Skip if priority != 0
        if item.get("priority") != 0:
            logger.debug("Skipping symbol due to priority != 0: %s", item.get("symbol"))
            continue

        symbol = item.get("symbol")
        if not symbol:
            logger.debug("Skipping index entry with no symbol: %s", item)
            continue

        # listing_date is now directly under meta_listingDate
        listing_date = item.get("meta_listingDate")  # e.g. "YYYY-MM-DD"

        # If listingDate is missing, we'll default to "2015-01-01" in update_or_create_symbol_entry
        symbol_prices_path = prices_template.format(symbol=symbol.upper()).lower()
        if os.path.exists(symbol_prices_path):
            earliest_stock_date, latest_stock_date = extract_symbol_dates_from_prices(symbol_prices_path)
        else:
            logger.info("Prices file not found for %s at %s. Using defaults.", symbol, symbol_prices_path)
            earliest_stock_date, latest_stock_date = "", ""

        update_or_create_symbol_entry(
            symbol=symbol,
            listing_date_str=listing_date if listing_date else "",
            earliest_stock_date=earliest_stock_date,
            latest_stock_date=latest_stock_date,
            metadata=existing_metadata
        )

    # 5) Save the updated metadata
    save_json_file(existing_metadata, metadata_file)
    logger.info("Metadata successfully updated in %s", metadata_file)


if __name__ == "__main__":
    main()
