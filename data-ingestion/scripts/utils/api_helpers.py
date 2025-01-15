import os
import json
import logging
from datetime import datetime, timedelta
import requests
from logging.handlers import RotatingFileHandler


# Configure logging
def configure_logging(log_file='logs/api.log'):
    """
    Configure logging for the module.

    Args:
        log_file (str): The log file path.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3),
            logging.StreamHandler()
        ]
    )


# Initialize logging
configure_logging()
logger = logging.getLogger(__name__)


def load_api_config(config_path):
    """
    Load the API configuration from a JSON file.
    """
    try:
        # Normalize the path and replace backslashes with forward slashes
        normalized_path = os.path.normpath(config_path).replace("\\", "/")
        
        with open(config_path, 'r') as file:
            api_config = json.load(file)
        logger.info("Successfully loaded API configuration from %s", normalized_path)
        return api_config
    except FileNotFoundError:
        logger.error("Configuration file not found: %s", config_path)
        raise
    except json.JSONDecodeError:
        logger.error("Invalid JSON in configuration file: %s", config_path)
        raise



def fetch_data_from_api(base_url, endpoint, params=None, headers=None):
    """
    Fetch data from an API endpoint, with built-in session and header handling.

    Args:
        base_url (str): The base URL for the API.
        endpoint (str): The specific endpoint to call.
        params (dict): Optional query parameters for the API request.
        headers (dict): Optional headers for the API request.

    Returns:
        dict: JSON response from the API.
    """
    url = f"{base_url}{endpoint}"
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive",
    }
    headers = headers or default_headers

    session = requests.Session()
    try:
        # Step 1: Load initial cookies
        session.get("https://www.nseindia.com", headers=headers)
        logger.info("Loaded initial cookies from NSE India.")

        # Step 2: Fetch the API data
        logger.info("Fetching data from API: %s", url)
        response = session.get(url, params=params, headers=headers)
        response.raise_for_status()
        logger.info("Successfully fetched data from API: %s", url)

        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data from API: %s. Error: %s", url, e)
        return None


def fetch_equity_stock_indices(api_config, index_name=None):
    """
    Fetch data for equity stock indices.

    Args:
        api_config (dict): API configuration dictionary.
        index_name (str): Name of the index to fetch data for.

    Returns:
        dict: JSON response for equity stock indices.
    """
    index_name = index_name or api_config['default_index_name']
    endpoint = api_config['endpoints']['equity_stock_indices'].format(index_name=index_name)

    logger.info("Fetching equity stock indices for index: %s", index_name)
    response = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    if response is None:
        logger.warning("Failed to fetch equity stock indices for index: %s", index_name)
    return response


def fetch_historical_security_archives(api_config, symbol, from_date=None, to_date=None):
    """
    Fetch historical security archives data.

    Args:
        api_config (dict): API configuration dictionary.
        symbol (str): The stock symbol.
        from_date (str): Start date in the format YYYY-MM-DD. Defaults to 365 days before today.
        to_date (str): End date in the format YYYY-MM-DD. Defaults to today.

    Returns:
        dict: JSON response for historical security archives.
    """
    to_date = to_date or datetime.now().strftime('%d-%m-%Y')
    from_date = from_date or (datetime.now() - timedelta(days=365)).strftime('%d-%m-%Y')

    endpoint = api_config['endpoints']['historical_security_archives'].format(
        from_date=from_date,
        to_date=to_date,
        symbol=symbol
    )

    logger.info("Fetching historical archives for symbol: %s, from: %s, to: %s", symbol, from_date, to_date)
    response = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    if response is None:
        logger.warning("Failed to fetch historical archives for symbol: %s, from: %s, to: %s", symbol, from_date, to_date)
    return response


def save_json_to_file(data, file_path):
    """
    Save a JSON object to a file.

    Args:
        data (dict): The JSON data to save.
        file_path (str): The file path to save the JSON data.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logger.info("Successfully saved JSON data to file: %s", file_path)
    except (OSError, IOError) as e:
        logger.error("Error saving JSON to file: %s. Error: %s", file_path, e)
        raise
