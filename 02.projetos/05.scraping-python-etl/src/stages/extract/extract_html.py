from src.drivers.interfaces.http_requester import HttpRequesterInteface
from src.drivers.interfaces.html_collector import HtmlCollectorInteface

class ExtractHtml:
    
    def __init__(self, http_requester: HttpRequesterInteface, html_collector: HtmlCollectorInteface) -> None:
        self.__http_requester = http_requester
        self.__html_collector = html_collector
        
    def extract(self):
        html_information = self.__http_requester.request_from_page()
        essential_information = self.__html_collector.collect_essential_information(html_information["html"])
        return essential_information
        