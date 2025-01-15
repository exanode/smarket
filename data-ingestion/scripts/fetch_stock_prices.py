import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote
from logging.handlers import RotatingFileHandler

from utils.api_helpers import load_api_config, fetch_historical_security_archives, save_json_to_file


def configure_logging(log_file: str = "fetch_historical_prices.log"):
    """
    Configure logging for the script with both console and file handlers.

    Args:
        log_file: Path to the log file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3),
            logging.StreamHandler()
        ]
    )


def parse_any_date(date_str: str) -> str:
    """
    Attempt to parse a date string from multiple potential formats
    and return it in 'DD-MM-YYYY' format.

    Args:
        date_str: Date string in any supported format 
                  (e.g., 'YYYY-MM-DD', 'DD-MM-YYYY', 'MM/DD/YYYY', etc.).

    Returns:
        The parsed date string in 'DD-MM-YYYY' format.

    Raises:
        ValueError: If the date string does not match any known formats.
    """
    possible_formats = [
        "%Y-%m-%d",  # 2023-01-01
        "%Y/%m/%d",  # 2023/01/01
        "%m/%d/%Y",  # 01/01/2023
        "%d-%m-%Y",  # 01-01-2023
        "%d/%m/%Y",  # 01/01/2023
    ]

    for fmt in possible_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%d-%m-%Y")
        except ValueError:
            pass

    raise ValueError(f"Unrecognized date format for '{date_str}'. Supported formats include: {possible_formats}")


def main():
    configure_logging()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Fetch historical stock prices for a specific symbol.")
    parser.add_argument('--symbol', required=True, help="Stock symbol to fetch data for.")
    parser.add_argument('--from_date', help="Start date in any common format (defaults to 1 year ago if omitted).")
    parser.add_argument('--to_date', help="End date in any common format (defaults to today if omitted).")
    parser.add_argument(
        '--output',
        default='data/stock_prices/{symbol}_historical_prices.json',
        help="Output file path (templated with {symbol})."
    )
    args = parser.parse_args()

    logger.info("Starting historical prices fetch for symbol: %s", args.symbol)

    # Dynamically resolve config path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', 'configs/api_config.json')

    if not os.path.exists(config_path):
        logger.error("Config file not found at %s", config_path)
        return

    api_config = load_api_config(config_path)

    # Determine final from_date / to_date in 'DD-MM-YYYY' format
    try:
        if args.from_date:
            from_date = parse_any_date(args.from_date)
        else:
            # Default: 1 year ago
            one_year_ago = datetime.now() - timedelta(days=365)
            from_date = one_year_ago.strftime("%d-%m-%Y")

        if args.to_date:
            to_date = parse_any_date(args.to_date)
        else:
            # Default: Today
            to_date = datetime.now().strftime("%d-%m-%Y")
    except ValueError as e:
        logger.error("Date parsing error: %s", e)
        return

    logger.info("Fetching data from %s to %s for symbol: %s", from_date, to_date, args.symbol)

    # Encode the symbol to handle special characters
    encoded_symbol = quote(args.symbol)

    try:
        # Fetch historical stock prices
        data = fetch_historical_security_archives(api_config, encoded_symbol, from_date, to_date)

        if data:
            output_path = args.output.format(symbol=args.symbol.upper())
            os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Ensure directory exists
            save_json_to_file(data, output_path)
            logger.info("Historical prices saved to %s", output_path)
        else:
            logger.warning("No data fetched for symbol: %s", args.symbol)
    except Exception as e:
        logger.exception("An error occurred while fetching historical prices for symbol: %s", args.symbol)


if __name__ == '__main__':
    main()
