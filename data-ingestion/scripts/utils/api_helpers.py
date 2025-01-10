import requests
import json
import os
from datetime import datetime, timedelta

def load_api_config(config_path):
    """
    Load the API configuration from a JSON file.
    """
    with open(config_path, 'r') as file:
        api_config = json.load(file)
    return api_config

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

        # Step 2: Fetch the API data
        response = session.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        return response.json()
    except requests.exceptions.RequestException as e:
        error_message = f"Error fetching data from API: {e}"
        print(error_message)
        log_api_error(error_message)  # Log the error for debugging
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
    response = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    if response is None:
        log_api_error(f"Failed to fetch equity stock indices for index: {index_name}")
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
    response = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    if response is None:
        log_api_error(f"Failed to fetch historical archives for symbol: {symbol}, from: {from_date}, to: {to_date}")
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
    except (OSError, IOError) as e:
        print(f"Error saving JSON to file: {e}")


def log_api_error(error_message, log_file='logs/api_errors.log'):
    """
    Log API errors to a log file.

    Args:
        error_message (str): The error message to log.
        log_file (str): The log file path.
    """
    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, 'a') as file:
            timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S') 
            file.write(f"[{timestamp}] {error_message}\n")
    except (OSError, IOError) as e:
        print(f"Error logging API error: {e}")