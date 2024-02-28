import sys
import pandas as pd
from fastparquet import ParquetFile, write


class ParquetHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def parse_parquet(self, selected_column):
        """
        Parse the parquet file and return the selected column as a pandas dataframe
        :param selected_column: The column to be selected from the parquet file
        :return: pandas dataframe
        """

        try:
            parquet_file = ParquetFile(self.file_path)
            return parquet_file.to_pandas()[selected_column]
        except:
            print("Error: Could not parse parquet file")
            sys.exit(1)

    def write_to_parquet(self, links_array):
        """
        Write the links to a parquet file
        :param links_array: The array of links to be written to the parquet file
        :return: None
        """
        try:
            df = pd.DataFrame(links_array, columns=["link"])
            write(self.file_path, df)
        except:
            print("Error: Could not write to parquet file")
            sys.exit(1)
