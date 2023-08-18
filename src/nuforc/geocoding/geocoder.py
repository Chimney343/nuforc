import csv
import logging
from geopy.geocoders import Nominatim
from tqdm import tqdm
import time

class Geolocator:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="geolocate_script")
        self.geolocation_data = []

    def geocode_with_retry(self, address):
        retries = 3
        for attempt in range(retries):
            try:
                location = self.geolocator.geocode(address)
                if location:
                    return location.latitude, location.longitude
            except Exception as e:
                logging.warning(f"Geocoding request for address: '{address}' failed. Retrying... Attempt {attempt + 1}")
                time.sleep(2)  # Adding a short delay before retrying
        return None, None

    def geolocate_addresses(self, addresses):
        for address in tqdm(addresses, desc="Geolocating", unit="address"):
            latitude, longitude = self.geocode_with_retry(address)
            if latitude is not None and longitude is not None:
                self.geolocation_data.append({
                    "address": address,
                    "latitude": latitude,
                    "longitude": longitude
                })
            else:
                logging.warning(f"Could not geolocate address: {address}")

    def write_to_csv(self, output_file):
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = ['Address', 'Latitude', 'Longitude']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(self.geolocation_data)

