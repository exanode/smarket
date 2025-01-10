import argparse
import subprocess

def run_fetch_stock_list(index_name, output_path):
    """
    Run the fetch_stock_list.py script.
    """
    command = [
        "python",
        "scripts/fetch_stock_list.py",
        "--index_name", index_name,
        "--output", output_path
    ]
    subprocess.run(command)

def run_fetch_stock_prices(symbol, from_date, to_date, output_path):
    """
    Run the fetch_stock_prices.py script.
    """
    command = [
        "python",
        "scripts/fetch_stock_prices.py",
        "--symbol", symbol,
        "--output", output_path
    ]
    if from_date:
        command += ["--from_date", from_date]
    if to_date:
        command += ["--to_date", to_date]
    subprocess.run(command)

def main():
    parser = argparse.ArgumentParser(description="Main ingestion script for data fetching.")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Subcommand: fetch_stock_list
    stock_list_parser = subparsers.add_parser("fetch_stock_list", help="Fetch stock list for an index")
    stock_list_parser.add_argument("--index_name", default="NIFTY 100", help="Name of the index (default: 'NIFTY 100')")
    stock_list_parser.add_argument("--output", default="data/indices/{index_name}_stock_list.json",
                                    help="Output file path for the stock list")
    
    # Subcommand: fetch_stock_prices
    stock_prices_parser = subparsers.add_parser("fetch_stock_prices", help="Fetch historical stock prices")
    stock_prices_parser.add_argument("--symbol", required=True, help="Stock symbol to fetch data for")
    stock_prices_parser.add_argument("--from_date", help="Start date (YYYY-MM-DD). Defaults to 1 year ago.")
    stock_prices_parser.add_argument("--to_date", help="End date (YYYY-MM-DD). Defaults to today.")
    stock_prices_parser.add_argument("--output", default="data/stock_prices/{symbol}_historical_prices.json",
                                     help="Output file path for historical prices")
    
    args = parser.parse_args()
    
    # Execute the selected command
    if args.command == "fetch_stock_list":
        output_path = args.output.format(index_name=args.index_name.replace(" ", "_"))
        run_fetch_stock_list(args.index_name, output_path)
    elif args.command == "fetch_stock_prices":
        output_path = args.output.format(symbol=args.symbol.upper())
        run_fetch_stock_prices(args.symbol, args.from_date, args.to_date, output_path)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()