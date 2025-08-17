from abc import ABC, abstractmethod
import pandas as pd

class SourceAdapter(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        ...
