from requests import session
from requests.adapters import HTTPAdapter, Retry


class NUFORC_HTTP_Client:
    def __init__(
        self,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0"
        },
        n_retries=10,
    ):
        self.session = session()

        # Set headers.
        self._headers = headers

        # Set retries.
        self._n_retries = Retry(
            total=n_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )

        self.session.mount("http://", HTTPAdapter(max_retries=self._n_retries))

    def get_response(self, url):
        response = self.session.get(url, headers=self._headers)
        return response
