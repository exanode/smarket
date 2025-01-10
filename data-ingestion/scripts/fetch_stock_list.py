import os
import argparse
from utils.api_helpers import load_api_config, fetch_data_from_api, save_json_to_file

def main():
    parser = argparse.ArgumentParser(description="Fetch stock list for a specific index.")
    parser.add_argument('--index_name', default=None, help="Name of the index. Defaults to 'NIFTY 100'.")
    parser.add_argument('--output', default='data/indices/{index_name}_stock_list.json', help="Output file path.")
    args = parser.parse_args()
    
    # Load API configuration
    config_path = 'configs/api_config.json'
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        return
    
    api_config = load_api_config(config_path)
    index_name = args.index_name or api_config.get('default_index_name', 'NIFTY 100')
    
    # Fetch stock list
    endpoint = api_config['endpoints']['equity_stock_indices'].format(index_name=index_name)
    data = fetch_data_from_api(api_config['nse_base_url'], endpoint)
    
    if data:
        output_path = args.output.format(index_name=index_name.replace(" ", "_"))
        save_json_to_file(data, output_path)
        print(f"Stock list saved to {output_path}")
    else:
        print(f"Failed to fetch stock list for index: {index_name}")

if __name__ == '__main__':
    main()