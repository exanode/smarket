import os
import shutil
from datetime import datetime, timedelta
from urllib.parse import quote
import json
import subprocess

def load_config(file_path):
    """Load configuration from a JSON file."""
    print(f"Loading configuration from {file_path}")
    with open(file_path, 'r') as f:
        return json.load(f)

def run_fetch_stock_list(index_name, output_path):
    """Run the fetch_stock_list.py script."""
    print(f"Fetching stock list for index: {index_name}")
    print(f"Saving stock list to: {output_path}")
    command = [
        "python",
        "scripts/fetch_stock_list.py",
        "--index_name", index_name,
        "--output", output_path
    ]
    subprocess.run(command, check=True)

def run_fetch_stock_prices(symbol, from_date, to_date, output_path):
    """Run the fetch_stock_prices.py script."""
    print(f"Fetching prices for stock: {symbol}")
    print(f"Saving stock prices to: {output_path}")
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
    subprocess.run(command, check=True)

def get_default_date_range():
    """Calculate the default date range (today - 365 to today)."""
    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    print(f"Default date range: {one_year_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
    return one_year_ago.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def extract_and_save_stock_names(stock_list_file, output_file):
    """
    Extract stock names from the stock list based on the 'priority' condition
    and save them to a JSON file.
    """
    print(f"Extracting stock names from {stock_list_file}")

    # Load the stock list JSON file
    with open(stock_list_file, 'r') as f:
        stock_list = json.load(f)  # Assuming the file contains a JSON object with 'data' key

    # Extract stock symbols where 'priority' is 0
    symbols = [stock['symbol'] for stock in stock_list['data'] if stock.get('priority') == 0]
    print(f"Found {len(symbols)} stock symbols with priority 0.")

    # Save stock names to a JSON file
    print(f"Saving stock names to {output_file}.")
    with open(output_file, 'w') as f:
        json.dump(symbols, f)
    
    return symbols


def archive_existing_file(file_path):
    """
    Archive an existing file to the archive/data/ folder if it exists.
    If an archived file with the same name exists, it will be overwritten.
    """
    if os.path.exists(file_path):
        # Create corresponding directory in the archive
        archive_dir = os.path.join("archive", os.path.dirname(file_path))
        os.makedirs(archive_dir, exist_ok=True)

        # Define the archive file path
        archive_file_path = os.path.join(archive_dir, os.path.basename(file_path))
        
        # Move the file to the archive folder
        shutil.move(file_path, archive_file_path)

        # Normalize paths for consistent output
        file_path_normalized = file_path.replace("\\", "/")
        archive_file_path_normalized = archive_file_path.replace("\\", "/")
        print(f"Archived existing file: {file_path_normalized} -> {archive_file_path_normalized}")
    else:
        print(f"No existing file to archive: {file_path}")


# Step 1: Fetch Stock List
def fetch_stock_list_step(config):
    """Fetch the stock list based on the index name."""
    print("Starting Step 1: Fetch Stock List.")
    index_name = config["index_name"]
    stock_list_output = config["output_paths"]["stock_list"].format(index_name=index_name.replace(" ", "_").lower())

    # Archive the existing file if it exists
    archive_existing_file(stock_list_output)

    # Fetch the stock list
    encoded_index_name = quote(index_name)  # URL-encode for API compatibility
    run_fetch_stock_list(encoded_index_name, stock_list_output)
    print(f"Stock list fetched and saved to {stock_list_output}")
    return stock_list_output

# Step 2: Extract and Save Stock Names
def extract_and_save_stock_names_step(stock_list_output, config):
    """Extract stock names from the stock list and save them."""
    print("Starting Step 2: Extract and Save Stock Names.")
    stock_names_file = config["output_paths"]["stock_names"]
    stock_names = extract_and_save_stock_names(stock_list_output, stock_names_file)
    print(f"Stock names saved to {stock_names_file}")
    return stock_names

# Step 3: Fetch Stock Prices
def fetch_stock_prices_step(stock_names, config):
    """Fetch stock prices for each stock symbol."""
    print("Starting Step 3: Fetch Stock Prices.")
    default_from_date, default_to_date = get_default_date_range()
    from_date = config["price_fetch_settings"].get("from_date", default_from_date)
    to_date = config["price_fetch_settings"].get("to_date", default_to_date)

    for symbol in stock_names:
        # Define the output path for stock prices
        stock_price_output = config["output_paths"]["stock_prices"].format(symbol=symbol.upper()).lower()

        # Archive existing stock price file if it exists
        archive_existing_file(stock_price_output)

        # Fetch and save stock prices
        run_fetch_stock_prices(symbol, from_date, to_date, stock_price_output)
    print("Completed Step 3: Stock Prices Fetched.")


def main(test_mode=False):
    print("Loading configuration and starting workflow.")

    # Load configuration from file
    config = load_config("config.json")

    # Step 1: Fetch Stock List
    stock_list_output = fetch_stock_list_step(config)
    
    if test_mode:
        print("Test mode enabled: Exiting after Step 1.")
        return

    # Step 2: Extract and Save Stock Names
    stock_names = extract_and_save_stock_names_step(stock_list_output, config)

    # Step 3: Fetch Stock Prices
    fetch_stock_prices_step(stock_names, config)

    print("Workflow completed successfully.")

if __name__ == "__main__":
    # Set test_mode=True to test only Step 1
    main(test_mode=True)