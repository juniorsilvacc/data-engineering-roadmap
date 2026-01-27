from .http_requester import HttpRequester

def test_request_from_page():
    http_requester = HttpRequester()
    http_requester.request_from_page()