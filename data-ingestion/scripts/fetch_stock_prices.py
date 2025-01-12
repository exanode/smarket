import os
import argparse
from datetime import datetime, timedelta
from typing import Optional

from utils.api_helpers import load_api_config, fetch_historical_security_archives, save_json_to_file

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

    # Load API configuration
    config_path = 'configs/api_config.json'
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        return

    api_config = load_api_config(config_path)

    # Determine final from_date / to_date in 'DD-MM-YYYY' format
    if args.from_date:
        try:
            from_date = parse_any_date(args.from_date)
        except ValueError as e:
            print(f"Error parsing from_date: {e}")
            return
    else:
        # Default: 1 year ago
        one_year_ago = datetime.now() - timedelta(days=365)
        from_date = one_year_ago.strftime("%d-%m-%Y")

    if args.to_date:
        try:
            to_date = parse_any_date(args.to_date)
        except ValueError as e:
            print(f"Error parsing to_date: {e}")
            return
    else:
        # Default: Today
        to_date = datetime.now().strftime("%d-%m-%Y")

    # Fetch historical stock prices
    data = fetch_historical_security_archives(api_config, args.symbol, from_date, to_date)

    if data:
        output_path = args.output.format(symbol=args.symbol.upper())
        save_json_to_file(data, output_path)
        print(f"Historical prices saved to {output_path}")
    else:
        print(f"Failed to fetch historical prices for symbol: {args.symbol}")


if __name__ == '__main__':
    main()
