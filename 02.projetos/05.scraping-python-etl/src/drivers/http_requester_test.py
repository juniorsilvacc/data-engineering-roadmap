from .http_requester import HttpRequester

def test_request_from_page():
    http_requester = HttpRequester()
    request_response = http_requester.request_from_page()

    print()
    print(request_response)