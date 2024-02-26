import requests
from fastparquet import ParquetFile
import sys

def parse_parquet_file(file, selected_column):
    try:
        parquet_file = ParquetFile(file)
        return parquet_file.to_pandas()[selected_column]
    except:
        print('Error: Could not parse parquet file')
        sys.exit(1)

def get_address(url):
    return

if __name__ == "__main__":
    file = open('list of company websites.snappy.parquet', 'rb')
    df = parse_parquet_file(file, 'domain')