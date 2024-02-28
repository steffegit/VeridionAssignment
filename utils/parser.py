import re
import requests
from bs4 import BeautifulSoup as bs
from geopy.geocoders import Nominatim


class AddressParser:
    def __init__(self, timeout=2):
        self.timeout = timeout
        self.geolocator = Nominatim(user_agent="veridionAssignment")
        self.zip_code_regex = re.compile(r"\b\d{5}(?:[-\s]\d{4})?\b")
        self.street_regex = re.compile(
            r"(?i)(^|\s)\d{2,7}\b\s+.{5,30}\b\s+(?:road|rd|way|street|st|str|avenue|ave|boulevard|blvd|lane|ln|drive|dr|terrace|ter|place|pl|court|ct)(?:\.|\s|$)"
        )
        self.po_box_regex = re.compile(r"(?i)(?:po|p.o.)\s+(?:box)")
        self.num_regex = re.compile(r"\d+")

    def create_final_address(self, location_from_street, location_from_zip):
        if not location_from_street and not location_from_zip:
            return None

        address_from_street = None
        address_from_zip = None

        if location_from_street:
            address_from_street = location_from_street.raw.get("address")
        if location_from_zip:
            address_from_zip = location_from_zip.raw.get("address")

        if not address_from_street and not address_from_zip:
            return None

        country = self.get_field_value(address_from_street, address_from_zip, "country")
        region = self.get_field_value(address_from_street, address_from_zip, "state")
        city = self.get_field_value(address_from_street, address_from_zip, "city")
        postcode = self.get_field_value(
            address_from_street, address_from_zip, "postcode"
        )
        road = self.get_field_value(address_from_street, address_from_zip, "road")
        house_number = self.get_field_value(
            address_from_street, address_from_zip, "house_number"
        )

        return {
            "country": country,
            "region": region,
            "city": city,
            "postcode": postcode,
            "road": road,
            "house_number": house_number,
        }

    def get_field_value(self, first, second, field):
        field1 = first.get(field) if first else None
        field2 = second.get(field) if second else None

        if field == "road" and field1:
            return field1

        return field2 or field1

    def get_location(self, soup, regex, url):
        try:
            for val in soup.find_all(string=regex):
                if len(val) <= 100:
                    location = re.search(regex, val).group(0)
                    location = re.sub(self.po_box_regex, "", location)
                    return location
        except AttributeError:
            print(f"AttributeError: Unable to find or process the location from {url}")
        except Exception as e:
            print(f"Unexpected error {e} occurred while getting location from {url}")
        return None

    def parse_address(self, url, user_agent):
        headers = {"User-Agent": user_agent}
        try:
            response = requests.get(
                url,
                timeout=self.timeout,
                headers=headers,
                allow_redirects=True,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as https_err:
            print(f"https error occurred while getting {url}")
            return
        except Exception as err:
            print(f"Error occurred while getting {url}")
            return

        try:
            soup = bs(response.text, "lxml")
        except Exception as e:
            print(f"Error occurred while parsing page {url}. Error: {e}")
            return

        street_address = self.get_location(soup, self.street_regex, url)
        zip_code = self.get_location(soup, self.zip_code_regex, url)

        location_from_street = None
        location_from_zip = None

        if street_address:
            try:
                location_from_street = self.geolocator.geocode(
                    street_address, addressdetails=True, timeout=self.timeout
                )
            except Exception as e:
                print(
                    f"Error validating location from the street_address {street_address} from page {url}. Error: {e}"
                )

        if zip_code:
            try:
                location_from_zip = self.geolocator.geocode(zip_code)
            except Exception as e:
                print(
                    f"Error validating location from the zip_code {zip_code} from page {url}. Error: {e}"
                )

        final_address = None
        try:
            final_address = self.create_final_address(
                location_from_street, location_from_zip
            )
        except Exception as e:
            print(f"Error getting final address from page {url}. Error {e}")

        if final_address:
            return {"domain": url.split("/")[2], "address": final_address}
        return None
