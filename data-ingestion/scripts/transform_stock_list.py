import os
import json
import argparse
import logging
from logging.handlers import RotatingFileHandler
import shutil

# --------------------------------------------------------------------------------
# Logging Configuration
# --------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("transform_stock_list.log", maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------------------------

def load_json_file(file_path: str) -> dict:
    """Load JSON data from a file."""
    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        return {}
    with open(file_path, 'r', encoding='utf-8') as f:
        logger.info("Loading JSON from %s", file_path)
        return json.load(f)

def save_json_file(data: dict, file_path: str) -> None:
    """Save JSON data to a file."""
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    logger.info("Saved JSON to %s", file_path)

def flatten_stock_item(item: dict) -> dict:
    """
    Flatten a single stock item, moving 'meta' sub-keys up as 'meta_<key>'.
    Example:
        {
            "symbol": "AAPL",
            "meta": { "listingDate": "2015-01-01", "someKey": "value" },
            "priority": 0
        }
        => {
            "symbol": "AAPL",
            "priority": 0,
            "meta_listingDate": "2015-01-01",
            "meta_someKey": "value"
        }
    """
    flattened = {}
    for key, value in item.items():
        if key == 'meta' and isinstance(value, dict):
            # Flatten 'meta' dict
            for mkey, mval in value.items():
                flattened[f"meta_{mkey}"] = mval
        else:
            # Keep top-level keys as is
            flattened[key] = value
    return flattened

def flatten_stock_list(stock_list: dict) -> list:
    """
    Flatten each stock record in stock_list['data'] by pulling nested 'meta' fields up.
    """
    flattened_list = []
    data = stock_list.get("data", [])
    for item in data:
        flattened_list.append(flatten_stock_item(item))
    return flattened_list

# --------------------------------------------------------------------------------
# Main Function
# --------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Flatten the stock list from --input JSON into --output JSON."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the original stock list JSON"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to the flattened output JSON"
    )
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output

    # 1) Load the original data
    stock_list = load_json_file(input_file)
    if not stock_list:
        logger.error("No valid data found in %s", input_file)
        return

    # 2) Backup the input file (optional)
    backup_path = f"{input_file}.backup"
    if os.path.exists(input_file):
        shutil.copy(input_file, backup_path)
        logger.info("Created backup of the original file at %s", backup_path)

    # 3) Flatten
    flattened_list = flatten_stock_list(stock_list)

    # 4) Save to a new output file
    save_json_file({"data": flattened_list}, output_file)
    logger.info("Flattening complete. Results saved to %s", output_file)

if __name__ == "__main__":
    main()
