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
# Constants
###############################################################################
DEFAULT_EARLIEST_DATE = "2015-01-01"
DATE_FMT = "%Y-%m-%d"  # Standard format: 'YYYY-MM-DD'
DMY_FMT = "%d-%m-%Y"   # Alternate format: 'DD-MM-YYYY'

###############################################################################
# Subprocess / Command Helpers
###############################################################################
def run_command(command: List[str], description: str) -> None:
    """
    Runs a command in a subprocess and logs its execution.

    Args:
        command (List[str]): Command to execute as a list of arguments.
        description (str): Description of the task for logging.

    Raises:
        subprocess.CalledProcessError: If the command fails.
    """
    logger.info("Executing: %s", " ".join(command))
    try:
        result = subprocess.run(
            command, check=True, text=True, capture_output=True
        )
        # Log stdout and stderr explicitly
        if result.stdout:
            logger.info("Output from %s:\n%s", description, result.stdout)
        if result.stderr:
            logger.warning("Errors from %s:\n%s", description, result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(
            "%s failed. Error:\n%s\nCommand: %s\nOutput: %s",
            description, e.stderr, e.cmd, e.output
        )
        raise



###############################################################################
# Date Parsing & Formatting
###############################################################################
def parse_date_dmy(date_str: str) -> Optional[datetime]:
    """
    Convert a string in 'DD-MM-YYYY' format into a datetime object.

    Args:
        date_str (str): A date string in 'DD-MM-YYYY' format.

    Returns:
        Optional[datetime]: The parsed datetime object, or None if empty/invalid.
    """
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, DMY_FMT)
    except ValueError:
        logger.warning("parse_date_dmy: Invalid date string '%s'", date_str)
        return None


def format_date_dmy(date_obj: datetime) -> str:
    """
    Format a datetime object as a 'DD-MM-YYYY' string.

    Args:
        date_obj (datetime): A datetime object to format.

    Returns:
        str: Date string in 'DD-MM-YYYY' format.
    """
    return date_obj.strftime(DMY_FMT)


def get_date_or_default(date_str: str, fallback: str) -> datetime:
    """
    Parses a date string in 'YYYY-MM-DD' or returns a default.

    Args:
        date_str (str): The date string to parse (expects 'YYYY-MM-DD').
        fallback (str): A fallback date in 'YYYY-MM-DD' format.

    Returns:
        datetime: Parsed date or fallback date if parsing fails.
    """
    try:
        return datetime.strptime(date_str, DATE_FMT)
    except (ValueError, TypeError):
        logger.warning("Invalid or missing date '%s'. Using fallback: %s", date_str, fallback)
        return datetime.strptime(fallback, DATE_FMT)

###############################################################################
# Config & Validation
###############################################################################
def load_config(file_path: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.

    Args:
        file_path (str): Path to the configuration file.

    Returns:
        Dict[str, Any]: Configuration dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    logger.info("Loading configuration from %s", file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate required config keys and log warnings for missing optional keys.

    Args:
        config (Dict[str, Any]): Configuration dictionary.

    Raises:
        KeyError: If mandatory keys are missing.
    """
    logger.info("Validating configuration...")
    required_keys = ["index_name", "output_paths", "price_fetch_settings"]
    output_keys = ["stock_list", "transformed_stock_list", "stock_prices", "stock_names"]
    price_keys = ["from_date", "to_date"]

    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config key: {key}")

    for key in output_keys:
        if key not in config["output_paths"]:
            raise KeyError(f"Missing output path key: output_paths.{key}")

    for key in price_keys:
        # Not necessarily fatal; we can use defaults
        if key not in config["price_fetch_settings"]:
            logger.warning("Missing price_fetch_settings key: %s. Defaults may apply.", key)


def dynamic_date_defaults(config: Dict[str, Any]) -> None:
    """
    Dynamically calculate default 'from_date' or 'to_date' if they're missing.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
    """
    logger.info("Applying dynamic date defaults (if needed)...")

    if not config["price_fetch_settings"].get("from_date"):
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime(DATE_FMT)
        config["price_fetch_settings"]["from_date"] = one_year_ago
        logger.info("Default from_date set to: %s", one_year_ago)

    if not config["price_fetch_settings"].get("to_date"):
        today = datetime.now().strftime(DATE_FMT)
        config["price_fetch_settings"]["to_date"] = today
        logger.info("Default to_date set to: %s", today)

###############################################################################
# JSON Merging
###############################################################################
def merge_json_data(existing_file: str, new_file: str) -> List[Dict[str, Any]]:
    """
    Merge data from new_file into existing_file (if it exists), deduplicate,
    and return the combined list of records.

    Args:
        existing_file (str): Path to the existing JSON file (may not exist).
        new_file (str): Path to the newly fetched JSON data.

    Returns:
        List[Dict[str, Any]]: Combined list of JSON records.
    """
    existing_data = []
    if os.path.exists(existing_file):
        with open(existing_file, 'r', encoding='utf-8') as ef:
            try:
                existing_data = json.load(ef)
                # If the JSON has a "data" key, assume we need that
                if isinstance(existing_data, dict) and "data" in existing_data:
                    existing_data = existing_data["data"]
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in %s; ignoring existing data.", existing_file)

    new_data = []
    with open(new_file, 'r', encoding='utf-8') as nf:
        try:
            new_data = json.load(nf)
            if isinstance(new_data, dict) and "data" in new_data:
                new_data = new_data["data"]
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in new data file %s: %s", new_file, e)

    # Deduplicate by JSON string
    combined = existing_data + new_data
    # Sort keys for consistent deduping
    combined = list({json.dumps(entry, sort_keys=True): entry for entry in combined}.values())
    return combined

###############################################################################
# Metadata (Read-Only)
###############################################################################
def load_symbol_metadata(metadata_file_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Load symbol metadata from a JSON file, returning a dict keyed by symbol.

    Args:
        metadata_file_path (str): Path to the metadata file (e.g. "symbol_metadata.json").

    Returns:
        Dict[str, Dict[str, Any]]: { 'TCS': {...}, 'INFY': {...}, ... }
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
# External Script Calls (Using run_command)
###############################################################################
def run_fetch_stock_list(index_name: str, output_path: str) -> None:
    """
    Fetch the stock list for the given index via an external Python script.
    """
    description = f"Fetching stock list for index '{index_name}'"
    command = [
        "python", "scripts/fetch_stock_list.py",
        "--index_name", index_name,
        "--output", output_path
    ]
    run_command(command, description)


def run_fetch_stock_prices(symbol: str, from_date: str, to_date: str, output_path: str) -> None:
    """
    Fetch stock prices for 'symbol' in 'from_date'..'to_date' range,
    merging new data with existing data at 'output_path'.
    """
    logger.info("Fetching prices for %s from %s to %s", symbol, from_date, to_date)

    # We'll fetch data into a temporary JSON file, then merge it.
    temp_output_path = f"{output_path}.tmp"
    description = f"Fetch stock prices for {symbol}"

    command = [
        "python", "scripts/fetch_stock_prices.py",
        "--symbol", symbol,
        "--output", temp_output_path
    ]
    if from_date:
        command += ["--from_date", from_date]
    if to_date:
        command += ["--to_date", to_date]

    # Run the external script
    try:
        run_command(command, description)
    except subprocess.CalledProcessError:
        # Already logged by run_command, just return
        return

    # Check if the temporary file was created
    if not os.path.exists(temp_output_path):
        logger.warning("No data file created for %s. Skipping merge.", symbol)
        return

    # Merge new data into existing data
    combined_data = merge_json_data(output_path, temp_output_path)

    # Save merged data
    with open(output_path, 'w', encoding='utf-8') as of:
        json.dump(combined_data, of, indent=4)
    logger.info("Merged & updated stock prices saved to %s", output_path)

    # Clean up temp file
    if os.path.exists(temp_output_path):
        os.remove(temp_output_path)


def run_transform_stock_list(input_file: str, output_file: str) -> None:
    """
    Transform the raw stock list (flatten fields, rename columns, etc.)
    """
    description = f"Transforming stock list {input_file} => {output_file}"
    command = [
        "python", "scripts/transform_stock_list.py",
        "--input", input_file,
        "--output", output_file
    ]
    run_command(command, description)


def run_populate_stock_metadata(config_file: str, metadata_file: str) -> None:
    """
    Build or update symbol metadata based on fetched data & transformations.
    """
    description = "Populating/updating symbol metadata"
    command = [
        "python", "scripts/populate_stock_metadata.py",
        "--config", config_file,
        "--metadata_file", metadata_file
    ]
    run_command(command, description)


def run_db_ingest() -> None:
    """
    Insert the latest data into the database.
    """
    description = "DB ingestion"
    command = ["python", "scripts/data_ingest.py"]
    run_command(command, description)

###############################################################################
# Chunked Fetch Logic
###############################################################################
def fetch_stock_prices_by_dates(symbol: str, start_date: datetime, end_date: datetime, output_path: str) -> None:
    """
    Fetch stock prices in yearly chunks from start_date to end_date, merging
    into 'output_path' as we go.

    Args:
        symbol (str): Stock symbol.
        start_date (datetime): Start date.
        end_date (datetime): End date.
        output_path (str): Path to the file where combined data is stored.
    """
    logger.info("Fetching historical prices for %s from %s to %s in yearly chunks...",
                symbol, start_date.strftime(DATE_FMT), end_date.strftime(DATE_FMT))

    current_date = start_date
    one_year = timedelta(days=365)

    while current_date <= end_date:
        chunk_end_date = min(current_date + one_year, end_date)
        run_fetch_stock_prices(
            symbol,
            # use d-m-y for the external script
            format_date_dmy(current_date),
            format_date_dmy(chunk_end_date),
            output_path
        )
        current_date = chunk_end_date + timedelta(days=1)

###############################################################################
# Main Steps
###############################################################################
def fetch_stock_list_step(config: Dict[str, Any]) -> str:
    """
    Fetch the stock list and return the path to the fetched JSON file.
    """
    index_name = config["index_name"]
    stock_list_template = config["output_paths"]["stock_list"]
    output_path = stock_list_template.format(index_name=index_name.replace(" ", "_"))

    run_fetch_stock_list(index_name, output_path)
    return output_path


def fetch_stock_prices_step(stock_names: List[str], config: Dict[str, Any]) -> None:
    """
    Fetch prices for each symbol, merging new data with existing data.
    Resumes from metadata if available.

    Args:
        stock_names (List[str]): List of stock symbols.
        config (Dict[str, Any]): Configuration dictionary.
    """
    logger.info("Fetching stock prices for %d symbols...", len(stock_names))

    today = datetime.now()
    from_date = get_date_or_default(
        config["price_fetch_settings"].get("from_date", ""), 
        DEFAULT_EARLIEST_DATE
    )
    to_date = get_date_or_default(
        config["price_fetch_settings"].get("to_date", ""), 
        today.strftime(DATE_FMT)
    )

    metadata_file_path = "symbol_metadata.json"
    metadata_dict = load_symbol_metadata(metadata_file_path)

    for symbol in stock_names:
        # Build output path for each symbol
        stock_prices_template = config["output_paths"]["stock_prices"]
        output_path = stock_prices_template.format(symbol=symbol.upper()).lower()

        # Determine start date from metadata
        md_entry = metadata_dict.get(symbol, {})
        if md_entry and md_entry.get("end_date"):
            try:
                # metadata end_date is 'YYYY-MM-DD'
                last_fetched = datetime.strptime(md_entry["end_date"], DATE_FMT)
                start_date = last_fetched + timedelta(days=1)
            except ValueError:
                logger.warning(
                    "Invalid end_date in metadata for %s. Using config from_date.", symbol
                )
                start_date = from_date
        else:
            start_date = from_date

        # Ensure we don't fetch beyond 'today'
        end_date = min(to_date, today)

        if start_date > end_date:
            logger.info("No new data to fetch for %s (start_date=%s, end_date=%s).",
                        symbol, start_date.strftime(DATE_FMT), end_date.strftime(DATE_FMT))
            continue

        # Fetch in yearly chunks
        fetch_stock_prices_by_dates(symbol, start_date, end_date, output_path)

    logger.info("Completed fetching stock prices for all symbols.")

###############################################################################
# Main Entry Point
###############################################################################
def main() -> None:
    """
    Main data ingestion pipeline for stock data:

       1. Load 'config.json'.
       2. Validate config, apply dynamic defaults.
       3. Fetch stock list for the configured index (e.g., 'NIFTY 100').
       4. Extract & save symbols with 'priority=0'.
       5. Fetch prices for each symbol (resume from metadata if found).
       6. Run external transform script to flatten/clean the fetched list.
       7. Populate/update symbol metadata.
       8. Run DB ingest to insert data into the database.
    """
    logger.info("Starting data ingestion pipeline...")

    config_path = "config.json"
    config = load_config(config_path)
    validate_config(config)
    dynamic_date_defaults(config)

    # Step 1: Fetch stock list
    stock_list_path = fetch_stock_list_step(config)

    # Step 2: Extract symbols with 'priority=0'
    with open(stock_list_path, 'r', encoding='utf-8') as f:
        stock_list_data = json.load(f)

    stock_names = [
        item["symbol"] 
        for item in stock_list_data.get("data", []) 
        if item.get("priority", 0) == 0
    ]

    # Save symbols to a JSON file
    stock_names_path = config["output_paths"]["stock_names"]
    with open(stock_names_path, 'w', encoding='utf-8') as f:
        json.dump(stock_names, f, indent=4)
    logger.info("Saved %d symbols to %s", len(stock_names), stock_names_path)

    # Step 3: Fetch prices for each symbol
    fetch_stock_prices_step(stock_names, config)

    # Step 4: Transform the fetched stock list
    transformed_stock_list_path = config["output_paths"]["transformed_stock_list"]
    run_transform_stock_list(stock_list_path, transformed_stock_list_path)

    # Step 5: Populate/update metadata
    metadata_file_path = "symbol_metadata.json"
    run_populate_stock_metadata(config_path, metadata_file_path)

    # Step 6: DB Ingestion
    run_db_ingest()
    logger.info("Data ingestion pipeline completed successfully.")

if __name__ == "__main__":
    main()
