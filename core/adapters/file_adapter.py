from pathlib import Path
import pandas as pd
from .base import SourceAdapter

class FileSourceAdapter(SourceAdapter):
    def __init__(self, path: str, sheet=None, header: int = 0, dtype=None):
        self.path = Path(path); self.sheet = sheet; self.header = header; self.dtype = dtype
    def extract(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        suf = self.path.suffix.lower()
        if suf in {".csv", ".txt"}:
            return pd.read_csv(self.path, dtype=self.dtype)
        return pd.read_excel(self.path, sheet_name=self.sheet, header=self.header, dtype=self.dtype)
