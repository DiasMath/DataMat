from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text


class DatabaseSourceAdapter:
    def __init__(self, source_url: str, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.source_url = source_url
        self.query = query
        self.params = dict(params or {}) # Os parâmetros vêm do Job

    def extract_raw(self) -> pd.DataFrame:
        """
        Executa a query no banco de dados de origem.
        Os parâmetros definidos no 'params' do Job são passados de forma segura
        para a query. Ex: SELECT * FROM tabela WHERE data > :data_inicio
        """
        engine = create_engine(self.source_url, future=True)
        with engine.begin() as conn:
            # O pandas passa o self.params para a query, substituindo os placeholders
            df = pd.read_sql(text(self.query), conn, params=self.params)
        return df