import re
import sys
import pandas as pd
from fastparquet import ParquetFile


class IOHandler:
    def format_field(field):
        if field is None:
            return ""

    def parse_parquet(self, file_path, selected_column):
        """
        Parse the parquet file and return the selected column as a pandas dataframe
        :param file_path: The path to the parquet file
        :param selected_column: The column to be selected from the parquet file
        :return: pandas dataframe
        """

        try:
            parquet_file = ParquetFile(file_path)
            return parquet_file.to_pandas()[selected_column]
        except:
            print("Error: Could not parse parquet file")
            sys.exit(1)

    def write_to_parquet(self, address_array):
        """
        Write the addresses to a parquet file
        :param address_array: The array of addresses to be written to the parquet file
        :param file_name: str
        """

        try:
            # Create a list of dictionaries, each representing a row of data
            data = []
            for element in address_array:
                row = {
                    "domain": element.get("domain"),
                    "country": element.get("address", {}).get("country", ""),
                    "region": element.get("address", {}).get("region", ""),
                    "city": element.get("address", {}).get("city", ""),
                    "postcode": element.get("address", {}).get("postcode", ""),
                    "road": element.get("address", {}).get("road", ""),
                    "house_number": element.get("address", {}).get("house_number", ""),
                }
                data.append(row)

            # Create a DataFrame from the list of dictionaries
            df = pd.DataFrame(data)

            # Write the DataFrame to a parquet file
            df.to_parquet("output/addresses.snappy.parquet", compression="snappy")
            print("Addresses written to addresses.snappy.parquet in the output folder.")
        except Exception as e:
            print("Error: Could not write to parquet file")
            print(str(e))
            sys.exit(1)

    def write_to_csv(self, address_array):
        """
        Write the addresses to a csv file
        :param address_array: The array of addresses to be written to the csv file
        """

        try:
            # Create a list of dictionaries, each representing a row of data
            data = []
            for element in address_array:
                row = {
                    "domain": self.format_field(element["domain"]),
                    "country": self.format_field(element["address"]["country"]),
                    "region": self.format_field(element["address"]["region"]),
                    "city": self.format_field(element["address"]["city"]),
                    "postcode": self.format_field(element["address"]["postcode"]),
                    "road": self.format_field(element["address"]["road"]),
                    "house_number": self.format_field(
                        element["address"]["house_number"]
                    ),
                }
                data.append(row)

            # Create a DataFrame from the list of dictionaries
            df = pd.DataFrame(data)

            # Write the DataFrame to a csv file
            df.to_csv("output/addresses.csv", index=False)
            print("Addresses written to addresses.csv in the output folder.")
        except Exception as e:
            print("Error: Could not write to csv file")
            print(str(e))
            sys.exit(1)
