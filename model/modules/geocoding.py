import logging
import os
import pickle
import time
from datetime import date
from pathlib import Path
from typing import List, Union

import pandas as pd
import requests
from tqdm.autonotebook import tqdm

logger = logging.getLogger("model.modules.geocoding")


class NUFORCGeocoder:
    available_input_types = ['.pkl']
    address_locations = ['city', 'state', 'state_abbreviation', 'country']
    supplementary_df = None
    output_df = None

    def __init__(
        self,
        input_df: Union[pd.DataFrame, Path],
        supplementary_df: Union[pd.DataFrame, Path] = None,
        api_key=os.getenv("API_KEY"),
        output_folder: Path() = Path.cwd(),
    ):
        assert input_df is not None, "Please provide either an input dataframe or path to file storing that dataframe."
        assert api_key is not None, "Please provide a Google Geocode API key under 'API_KEY' environment variable."
        assert Path(output_folder).is_dir(), "Please provide a viable folder for saving results."
        self.input_df = self._load_input_df(input_df=input_df)
        if supplementary_df:
            self.supplementary_df = self._load_supplementary_df(supplementary_df=supplementary_df)
        self.output_folder = Path(output_folder)
        self.api_key = api_key
        self.geocoded_results = []

        # Engineer unique locations to geocode.
        self._make_unique_locations_df(input_df=self.input_df, supplementary_df=self.supplementary_df)

    def _get_input_type(self, input: Path()):
        input_path = Path(input)
        input_type = input_path.suffix
        assert (
            input_type in self.available_input_types
        ), f"Filetype -> {input_type} cannot be used. Available input types are: {self.available_input_types}"
        return input_type

    def _load_pickle(self, input_df_path):
        with open(input_df_path, 'rb') as pickle_file:
            data = pickle.load(pickle_file)
            return pd.DataFrame(data)

    def _load_input_df(self, input_df):
        if isinstance(input_df, pd.DataFrame):
            df = input_df
        elif isinstance(input_df, (str, Path)):
            input_df_path = Path(input_df)
            input_df_type = self._get_input_type(input_df_path)
            if input_df_type == '.pkl':
                df = self._load_pickle(input_df_path=input_df_path)
                logger.info(f"Input dataframe loaded from {input_df}.")
        return df

    def _load_supplementary_df(self, supplementary_df):
        if isinstance(supplementary_df, pd.DataFrame):
            df = supplementary_df
        elif isinstance(supplementary_df, (str, Path)):
            input_df_path = Path(supplementary_df)
            input_df_type = self._get_input_type(input_df_path)
            if input_df_type == '.pkl':
                data = self._load_pickle(input_df_path=input_df_path)

            df = pd.DataFrame(data)
            logger.info(f'Supplementary results loaded from {input_df_path}')
        return df

    def _make_unique_locations_df(self, input_df, supplementary_df):
        assert isinstance(input_df, pd.DataFrame), "Need to provide input dataframe."
        df = input_df[self.address_locations].copy()
        df.drop_duplicates(inplace=True)
        if supplementary_df is not None:
            common_cols = [col for col in supplementary_df.columns if col in input_df.columns]
            df = df.merge(supplementary_df, on=common_cols, how='left')
        self.unique_locations_df = df

    def _geocode_single_address(self, raw_address, api_key):
        logger.debug(f"Geocoding {raw_address}...")
        geocoder = Geocoder(raw_address=raw_address, google_api_key=api_key)
        geocoder.geocode()
        return geocoder.results

    def _save_geocoded_results(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')
        results_path = path / f"geocoded_results.pkl"

        with open(results_path, "wb") as f:
            pickle.dump(self.geocoded_results, f)
        logger.info(f"Geocoded results saved @ {results_path}.")

    def _save_geocoded_df(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')
        geocoded_df_path = path / f"geocoded_df.pkl"

        with open(geocoded_df_path, "wb") as f:
            pickle.dump(self.geocoded_df, f)
        logger.info(f"Geocoded dataframe saved @ {geocoded_df_path}.")

    def geocode(self):
        assert (
            self.unique_locations_df is not None
        ), "Couldn't access unique locations dataframe. Check if you've properly loaded the input dataframe."

        # Empty the geocoded_results container.
        self.geocoded_results = []

        # Initialize a unique_locations_df hard copy.
        df = self.unique_locations_df.copy()
        if self.supplementary_df is not None:
            n_known_locations = len(df.loc[~(df['latitude'].isna()) & (~df['longitude'].isna())])
            df = df.loc[(df['latitude'].isna()) & (df['longitude'].isna())][self.address_locations]
            logger.info(f"Found {n_known_locations} locations within supplementary dataframe.")

        if len(df) == 0:
            logger.info("All submitted locations found in supplementary df.")
            self.geocoded_df = self.supplementary_df
            self.output_df = self.input_df.merge(self.geocoded_df, on=self.address_locations, how='left')
            self._save_geocoded_df()

        else:
            logger.info(f"Found {len(df)} unique locations to geocode.")
            for _, row in tqdm(df.iterrows(), total=len(df)):
                raw_address = list(row.values)
                self.geocoded_results.append(
                    self._geocode_single_address(raw_address=raw_address, api_key=self.api_key)
                )

            geocoded_df = pd.DataFrame(self.geocoded_results)
            geocoded_df = pd.DataFrame(geocoded_df['raw_address'].to_list(), columns=self.address_locations).merge(
                geocoded_df[geocoded_df.columns[1:]], left_index=True, right_index=True, how='left'
            )

            if self.supplementary_df is not None:
                self.geocoded_df = pd.concat([geocoded_df, self.supplementary_df])
            else:
                self.geocoded_df = geocoded_df

            self.output_df = self.input_df.merge(self.geocoded_df, on=self.address_locations, how='left')
            self._save_geocoded_results()
            self._save_geocoded_df()

    def save_events(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        date_today = date.today().strftime('%Y_%m_%d')

        filename_path = path / f"events_{date_today}.pkl"
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
            if results['status'] == "OK":
                results['latitude'] = response_json['results'][0]['geometry']['location']['lat']
                results['longitude'] = response_json['results'][0]['geometry']['location']['lng']
            else:
                results['latitude'] = None
                results['longitude'] = None
        return results

    def geocode(self):
        """
        Performs the main workflow and stores geocoding results as object attributes.
        """
        self.response = self.get_geocode_api_response()
        self.results = self.wrap_response(response=self.response)
