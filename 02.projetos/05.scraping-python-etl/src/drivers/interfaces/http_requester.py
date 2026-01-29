from typing import Dict
from abc import ABC, abstractmethod

class HttpRequesterInteface(ABC):

    @abstractmethod
    def request_from_page(self) -> Dict[int, str | int]:
        pass