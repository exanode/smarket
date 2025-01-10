import os
import argparse
from datetime import datetime
from utils.api_helpers import load_api_config, fetch_historical_security_archives, save_json_to_file

def main():
    parser = argparse.ArgumentParser(description="Fetch historical stock prices for a specific symbol.")
    parser.add_argument('--symbol', required=True, help="Stock symbol to fetch data for.")
    parser.add_argument('--from_date', help="Start date (YYYY-MM-DD). Defaults to 1 year ago.")
    parser.add_argument('--to_date', help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument('--output', default='data/stock_prices/{symbol}_historical_prices.json', help="Output file path.")
    args = parser.parse_args()

    # Load API configuration
    config_path = 'configs/api_config.json'
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        return

    api_config = load_api_config(config_path)

    # Convert dates to DD-MM-YYYY format
    from_date = args.from_date
    to_date = args.to_date

    if from_date:
        try:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').strftime('%d-%m-%Y')
        except ValueError:
            print(f"Error: Invalid format for from_date. Expected YYYY-MM-DD, got {from_date}")
            return

    if to_date:
        try:
            to_date = datetime.strptime(to_date, '%Y-%m-%d').strftime('%d-%m-%Y')
        except ValueError:
            print(f"Error: Invalid format for to_date. Expected YYYY-MM-DD, got {to_date}")
            return

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
