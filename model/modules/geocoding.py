import os
import pickle
from datetime import date
from typing import List
import pandas as pd
import requests
import time
from pathlib import Path
from typing import Union

import logging

logger = logging.getLogger("model.modules.geocoding")


class NUFORCGeocoder:
    available_input_types = ['.pkl']

    def __init__(
        self,
        input_df: Union[pd.DataFrame, Path],
        supplementary_results: Union[pd.DataFrame, Path] = None,
        api_key=os.getenv("API_KEY"),
        output_folder: Path() = Path.cwd(),
    ):
        assert input_df is not None, "Please provide either an input dataframe or path to file storing that dataframe."
        assert api_key is not None, "Please provide a Google Geocode API key under 'API_KEY' environment variable."
        assert Path(output_folder).is_dir(), "Please provide a viable folder for saving results"
        self.input_df = self._load_input_df(input_df=input_df)
        self._make_unique_locations_df(input_df=self.input_df)
        if supplementary_results:
            self.supplementary_results = self._load_supplementary_results(supplementary_results=supplementary_results)
        self.output_folder = Path(output_folder)
        self.api_key = api_key
        self.geocoded_results = []

    def _get_input_type(self, input: Path()):
        input_path = Path(input)
        input_type = input_path.suffix
        assert (
            input_type in self.available_input_types
        ), f"Filetype -> {input_type} cannot be used. Available input types are: {self.available_input_types}"
        return input_type

    def _load_pickle(self, df_input):
        with open(df_input, 'rb') as pickle_file:
            data = pickle.load(pickle_file)
            return pd.DataFrame(data)

    def _load_input_df(self, input_df):
        if isinstance(input_df, pd.DataFrame):
            return input_df
        elif isinstance(input_df, (str, Path)):
            try:
                input_df_path = Path(input_df)
                if not input_df_path.exists():
                    raise OSError(f"{input_df} does not exists or isn't accessible.")
            except:
                raise TypeError(f"{input_df_path} is not a valid filepath.")

            df_input_type = self._get_input_type(input_df_path)
            if df_input_type == '.pkl':
                return self._load_pickle(df_input=input_df_path)

    def _load_supplementary_results(self, supplementary_results):
        if isinstance(supplementary_results, pd.DataFrame):
            return supplementary_results
        elif isinstance(supplementary_results, (str, Path)):
            try:
                path = Path(supplementary_results)
                if not path.exists():
                    raise TypeError(f"Supplementary results {path} does not exists or isn't accessible.")
            except:
                raise TypeError(f"{path} is not a valid filepath.")
            path_filetype = self._get_input_type(path)
            if path_filetype == '.pkl':
                data = self._load_pickle(df_input=path)

            df = pd.DataFrame(data)
            df = pd.DataFrame(
                df['raw_address'].to_list(), columns=self.unique_location_parts
            ).merge(df[df.columns[1:]], left_index=True, right_index=True)
            logger.info(f'Supplementary results loaded from {path}')
            return df

    def _make_unique_locations_df(self, input_df):
        assert isinstance(input_df, pd.DataFrame), "Need to provide input dataframe."
        locations = ['city', 'state', 'state_abbreviation', 'country']
        cols_to_select = [col for col in input_df.columns if col in locations]
        df = input_df[cols_to_select].copy()
        df.drop_duplicates(inplace=True)
        self.unique_location_parts = cols_to_select
        self.unique_locations_df = df

    def _geocode_single_address(self, raw_address, api_key):
        logger.info(f"Geocoding {raw_address}...")
        geocoder = Geocoder(raw_address=raw_address, google_api_key=api_key)
        geocoder.geocode()
        return geocoder.results

    def _save_geocoded_results(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')
        filename_path = path / f"geocoded_results_{date_today}.pkl"
        with open(filename_path, "wb") as f:
            pickle.dump(self.geocoded_results, f)

    def geocode(self):
        # Ensure geocoding results container is empty.
        self.geocoded_results = []
        assert (
            self.unique_locations_df is not None
        ), "Couldn't access unique locations dataframe. Check if you've properly loaded the input dataframe."
        logger.info(f"Found {len(self.unique_locations_df)} unique locations to geocode.")
        for row in self.unique_locations_df.iterrows():
            raw_address = row[1].values
            self.geocoded_results.append(self._geocode_single_address(raw_address=raw_address, api_key=self.api_key))

        geocoded_df = pd.DataFrame(self.geocoded_results)
        geocoded_df = pd.DataFrame(geocoded_df['raw_address'].to_list(), columns=self.unique_location_parts).merge(
            geocoded_df[geocoded_df.columns[1:]], left_index=True, right_index=True
        )
        self.geocoded_df = geocoded_df
        self.output_df = self.input_df.merge(self.geocoded_df, on=self.unique_location_parts)
        self._save_geocoded_results()

    def save_events(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')

        filename_path = path / f"events_{date_today}.pkl"
        metadata_filename = path / f"events_metadata_{date_today}.pkl"
        with open(filename_path, "wb") as f:
            pickle.dump(self.events, f)


class Geocoder:
    def __init__(
        self,
        raw_address: List = [],
        google_api_key: str = os.getenv("API_KEY"),
        output_format="json",
        language="en",
        sleep_time=0.033,
    ):
        self.raw_address = raw_address
        self.google_api_key = google_api_key
        self.output_format = output_format
        self.language = language
        self.sleep_time = sleep_time
        self.call = self._make_geocode_api_call()

    def _make_geocode_api_call(self):
        address_parts = [part.replace(" ", "+") for part in self.raw_address if type(part) == str]
        if len(address_parts) <= 1:
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

    def wrap_response(self, response):
        """
        Wrangles the input/call/output information into final geocoding results.
        """
        results = {'raw_address': self.raw_address}
        if response.status_code == 200:
            response_json = response.json()
            results['response'] = response_json
            results['status'] = response_json.get('status')
            results['latitude'] = response_json['results'][0]['geometry']['location']['lat']
            results['longitude'] = response_json['results'][0]['geometry']['location']['lng']
        return results

    def geocode(self):
        """
        Performs the main workflow and stores geocoding results as object attributes.
        """
        self.response = self.get_geocode_api_response()
        self.results = self.wrap_response(response=self.response)
