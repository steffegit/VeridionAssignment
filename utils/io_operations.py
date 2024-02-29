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

    def write_to_parquet(self, path, links_array):
        """
        Write the links to a parquet file
        :param links_array: The array of links to be written to the parquet file
        :return: None
        """
        try:
            df = pd.DataFrame(
                links_array,
                columns=[
                    "domain",
                    "country",
                    "state",
                    "city",
                    "zipcode",
                    "street",
                    "house number",
                ],
            )
            write(path, df)
        except:
            print("Error: Could not write to parquet file")
            # sys.exit(1)

    def write_to_csv(self, path, address_array):
        """
        Write the addresses to a csv file
        :param address_array: The array of addresses to be written to the csv file
        :return: None
        """
        try:
            df = pd.DataFrame(
                address_array,
                columns=[
                    "domain",
                    "country",
                    "state",
                    "city",
                    "zipcode",
                    "street",
                    "house number",
                ],
            )
            df.to_csv(path, index=False)
        except:
            print("Error: Could not write to csv file")
            # sys.exit(1)
