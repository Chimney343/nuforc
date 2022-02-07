import os
from typing import List
import requests
import time


class Geocoder:
    def __init__(
        self,
        raw_address: List = [],
        google_api_key: str = os.getenv("GOOGLE_API_KEY"),
        output_format="json",
        language="en",
        sleep_time=0.033,
    ):
        self.raw_address = raw_address
        self.google_api_key = google_api_key
        self.output_format = output_format
        self.language = language
        self.sleep_time = sleep_time
        self.call = self.__make_geocode_api_call()

    def __make_geocode_api_call(self):
        address_parts = [
            part.replace(" ", "+") for part in self.raw_address if type(part) == str
        ]
        if len(address_parts) <=1:
            address = address_parts[0]
        else:
            address = ','.join(address_parts)

        call = f"https://maps.googleapis.com/maps/api/geocode/{self.output_format}?address={address}&language={self.language}&key={self.google_api_key}"
        return call

    def get_geocode_api_response(self):
        """
        Calls the Google Geocode API for a response.
        :return:
        """
        response = requests.get(self.call)
        # By default, Google Geocode API accepts up to 50 calls per second. Timeout below (1/30 of a second) ensure API
        # isn't blocked.t
        time.sleep(self.sleep_time)
        return response

    def validate_geocoder_response(self, response):
        """
        Validates if the geocoder response has been correct.
        """
        pass

    def make_geocoder_results(self):
        """
        Wrangles the input/call/output information into final geocoding results.
        """
        pass

    def geocode(self):
        """
        Performs the main workflow and stores geocoding results as object attributes.
        """
        response = self.get_geocode_api_response()
        validate_response = self.validate_geocoder_response()
        results = self.make_geocoder_results()



