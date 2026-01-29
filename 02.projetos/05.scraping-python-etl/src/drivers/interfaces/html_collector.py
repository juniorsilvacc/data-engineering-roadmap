from typing import List, Dict
from abc import ABC, abstractmethod

class HtmlCollectorInteface(ABC):

    @abstractmethod
    def collect_essential_information(self, html: str) -> List[Dict[str, str]]:
        pass
