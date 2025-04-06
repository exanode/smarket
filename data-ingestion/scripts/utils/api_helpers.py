import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Union
import requests
from logging.handlers import RotatingFileHandler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def configure_logging(log_file: str = 'logs/api.log', log_level: int = logging.INFO) -> None:
    """
    Configure application-wide logging with rotation and console output.
    
    Creates the log directory if it doesn't exist and sets up both file and console handlers.
    
    Args:
        log_file: Path to the log file (default: 'logs/api.log')
        log_level: Logging level to use (default: logging.INFO)
    
    Returns:
        None
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicate logging
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    # Add rotating file handler
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


# Initialize logging
configure_logging()
logger = logging.getLogger(__name__)


def load_api_config(config_path: str) -> Dict[str, Any]:
    """
    Load and validate API configuration from a JSON file.
    
    Args:
        config_path: Path to the JSON configuration file
    
    Returns:
        Dict containing the API configuration
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        json.JSONDecodeError: If the configuration file contains invalid JSON
        ValueError: If required configuration fields are missing
    """
    try:
        normalized_path = os.path.normpath(config_path).replace("\\", "/")
        
        with open(normalized_path, 'r') as file:
            api_config = json.load(file)
        
        # Validate essential configuration fields
        required_fields = ['nse_base_url', 'endpoints', 'default_index_name']
        missing_fields = [field for field in required_fields if field not in api_config]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
            
        # Validate endpoints structure
        if not isinstance(api_config['endpoints'], dict):
            raise ValueError("'endpoints' must be a dictionary")
            
        required_endpoints = ['equity_stock_indices', 'historical_security_archives']
        missing_endpoints = [ep for ep in required_endpoints if ep not in api_config['endpoints']]
        
        if missing_endpoints:
            raise ValueError(f"Missing required endpoints in configuration: {', '.join(missing_endpoints)}")
        
        logger.info("Successfully loaded API configuration from %s", normalized_path)
        return api_config
        
    except FileNotFoundError:
        logger.error("Configuration file not found: %s", config_path)
        raise
    except json.JSONDecodeError:
        logger.error("Invalid JSON in configuration file: %s", config_path)
        raise
    except ValueError as e:
        logger.error("Invalid configuration: %s", str(e))
        raise


def create_session() -> requests.Session:
    """
    Create and configure a requests session with retry logic.
    
    Returns:
        A configured requests.Session object with retry capabilities
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,  # Maximum number of retries
        backoff_factor=1,  # Time factor between retries (1s, 2s, 4s)
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]  # Only retry GET requests
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_data_from_api(
    base_url: str, 
    endpoint: str, 
    params: Optional[Dict[str, str]] = None, 
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    max_retries: int = 3
) -> Optional[Dict[str, Any]]:
    """
    Fetch data from an API endpoint with robust error handling and retry logic.
    
    First loads initial cookies from NSE India website before making the actual API request.
    Implements retries with exponential backoff for transient failures.
    
    Args:
        base_url: The base URL of the API
        endpoint: The specific API endpoint to call
        params: Optional query parameters for the request
        headers: Optional custom headers to include in the request
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of manual retries for non-status code failures (default: 3)
    
    Returns:
        JSON response data as a dictionary, or None if the request failed
    """
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    
    # Updated User-Agent to a more recent browser
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/get-quotes/equity?symbol=JSWSTEEL",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive"
    }

    # Merge default headers with custom headers, with custom headers taking precedence
    merged_headers = {**default_headers, **(headers or {})}
    
    session = create_session()
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            # Step 1: Load initial cookies from multiple pages
            try:
                # First visit homepage
                logger.info("Loading cookies from NSE homepage")
                homepage_response = session.get(
                    "https://www.nseindia.com", 
                    headers=merged_headers,
                    timeout=timeout
                )
                homepage_response.raise_for_status()
                
                # Wait a bit to simulate human behavior
                time.sleep(3)
                
                # Then visit the quotes page for the symbol
                logger.info("Loading cookies from stock quote page")
                symbol = params.get('symbol', 'NIFTY') if params else 'NIFTY'
                quotes_response = session.get(
                    f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
                    headers=merged_headers,
                    timeout=timeout
                )
                quotes_response.raise_for_status()
                
                # Wait again before making the API call
                time.sleep(2)
                
                logger.info("Successfully loaded cookies from NSE India")
                logger.debug("Session cookies: %s", session.cookies.get_dict())
            except requests.exceptions.RequestException as e:
                logger.warning("Failed to load initial cookies: %s", str(e))
                # Continue anyway as the main request might still work
            
            # Step 2: Fetch the API data
            logger.info("Fetching data from API: %s", url)
            response = session.get(
                url, 
                params=params, 
                headers=merged_headers,
                timeout=timeout
            )
            response.raise_for_status()
            
            # Check if response is valid JSON
            data = response.json()
            logger.info("Successfully fetched data from API: %s", url)
            return data
            
        except requests.exceptions.JSONDecodeError as e:
            logger.error("Invalid JSON in API response: %s. Error: %s", url, str(e))
            retry_count += 1
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error fetching data from API: {url}. Error: {str(e)}"
            
            # Check if we should retry or give up
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff: 1s, 2s, 4s, etc.
                logger.warning(
                    "%s. Retrying in %s seconds (attempt %s/%s)...", 
                    error_msg, wait_time, retry_count + 1, max_retries
                )
                time.sleep(wait_time)
                retry_count += 1
            else:
                logger.error("%s. Maximum retries reached.", error_msg)
                return None
    
    logger.error("Failed to fetch data after %s attempts: %s", max_retries + 1, url)
    return None


def fetch_equity_stock_indices(
    api_config: Dict[str, Any], 
    index_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetch data for equity stock indices from NSE.
    
    Retrieves current market data for the specified stock index.
    
    Args:
        api_config: API configuration dictionary containing endpoints and base URLs
        index_name: Name of the index to fetch (uses default from config if not specified)
    
    Returns:
        Dictionary containing index data, or None if the request failed
        
    Example:
        >>> config = load_api_config("config.json")
        >>> nifty_data = fetch_equity_stock_indices(config, "NIFTY 50")
        >>> print(nifty_data["metadata"]["indexName"])
        NIFTY 50
    """
    # Validate input
    if not isinstance(api_config, dict):
        logger.error("Invalid api_config: expected dictionary")
        return None
        
    # Use default index if none provided
    try:
        index_name = index_name or api_config['default_index_name']
    except KeyError:
        logger.error("Missing 'default_index_name' in api_config")
        return None
        
    try:
        endpoint = api_config['endpoints']['equity_stock_indices'].format(index_name=index_name)
    except KeyError:
        logger.error("Missing required endpoint 'equity_stock_indices' in api_config")
        return None
    except Exception as e:
        logger.error("Error formatting endpoint: %s", str(e))
        return None

    logger.info("Fetching equity stock indices for index: %s", index_name)
    response = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    
    if response is None:
        logger.warning("Failed to fetch equity stock indices for index: %s", index_name)
        
    return response


def fetch_historical_security_archives(
    api_config: Dict[str, Any],
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetch historical price data for a specific security.
    
    Args:
        api_config: API configuration dictionary containing endpoints and base URLs
        symbol: The stock symbol to fetch data for (e.g., "TATAMOTORS")
        from_date: Start date in DD-MM-YYYY format (defaults to 1 year ago)
        to_date: End date in DD-MM-YYYY format (defaults to current date)
    
    Returns:
        Dictionary containing historical price data, or None if the request failed
        
    Example:
        >>> config = load_api_config("config.json")
        >>> historical_data = fetch_historical_security_archives(config, "TATAMOTORS")
        >>> print(len(historical_data["data"]))  # Number of days of data
        252
    """
    # Input validation
    if not symbol:
        logger.error("Symbol cannot be empty")
        return None
        
    if not isinstance(api_config, dict):
        logger.error("Invalid api_config: expected dictionary")
        return None
    
    # Format dates
    today = datetime.now()
    
    if to_date:
        try:
            # Parse and validate to_date if provided
            datetime.strptime(to_date, '%d-%m-%Y')
        except ValueError:
            logger.error("Invalid to_date format. Expected DD-MM-YYYY, got: %s", to_date)
            return None
    else:
        to_date = today.strftime('%d-%m-%Y')
    
    if from_date:
        try:
            # Parse and validate from_date if provided
            datetime.strptime(from_date, '%d-%m-%Y')
        except ValueError:
            logger.error("Invalid from_date format. Expected DD-MM-YYYY, got: %s", from_date)
            return None
    else:
        # Default to 1 year ago
        from_date = (today - timedelta(days=365)).strftime('%d-%m-%Y')
    
    try:
        # Instead of using format, manually construct the endpoint with parameters to pass to fetch_data_from_api
        base_endpoint = api_config['endpoints']['historical_security_archives']
        
        # Remove any existing query parameters
        if '?' in base_endpoint:
            base_endpoint = base_endpoint.split('?')[0]
            
        # Define parameters separately
        params = {
            'from': from_date,
            'to': to_date,
            'symbol': symbol,
            'dataType': 'priceVolumeDeliverable',
            'series': 'ALL'
        }
        
    except KeyError:
        logger.error("Missing required endpoint 'historical_security_archives' in api_config")
        return None
    except Exception as e:
        logger.error("Error preparing endpoint: %s", str(e))
        return None
    
    logger.info("Fetching historical archives for symbol: %s, from: %s, to: %s", 
                symbol, from_date, to_date)
                
    response = fetch_data_from_api(
        api_config['nse_base_url'], 
        base_endpoint, 
        params=params,
        headers={
            "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        }
    )
    
    if response is None:
        logger.warning("Failed to fetch historical archives for symbol: %s", symbol)
        
    return response


def save_json_to_file(
    data: Any, 
    file_path: str, 
    indent: int = 4,
    ensure_ascii: bool = False
) -> bool:
    """
    Save data to a JSON file with error handling.
    
    Creates necessary directories if they don't exist.
    
    Args:
        data: The data to serialize to JSON and save
        file_path: The path to save the file to
        indent: Number of spaces for indentation (default: 4)
        ensure_ascii: Whether to escape non-ASCII characters (default: False)
    
    Returns:
        True if save was successful, False otherwise
        
    Example:
        >>> data = {"results": [{"name": "Example", "value": 123}]}
        >>> save_json_to_file(data, "output/results.json")
        True
    """
    try:
        # Ensure the directory exists
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        # Serialize and save the data
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=indent, ensure_ascii=ensure_ascii)
            
        logger.info("Successfully saved JSON data to file: %s", file_path)
        return True
        
    except (OSError, IOError) as e:
        logger.error("Error saving JSON to file: %s. Error: %s", file_path, str(e))
        return False
        
    except TypeError as e:
        logger.error("Error serializing data to JSON: %s", str(e))
        return False