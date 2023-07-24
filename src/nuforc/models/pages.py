from ..client import NUFORC_HTTP_Client
from requests import Response


class NUFORCBasicPage:
    def __init__(self, page_response: Response):
        self.page_response = page_response
        self.page_response_status_code = page_response.status_code


class NUFORCMonthlyEventsRootPage(NUFORCBasicPage):
    def __init__(self, page):
        super().__init__(page)
