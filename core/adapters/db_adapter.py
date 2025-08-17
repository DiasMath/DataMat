from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text


class DatabaseSourceAdapter:
    def __init__(self, source_url: str, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.source_url = source_url
        self.query = query
        self.params = dict(params or {})
        self._inc_field: Optional[str] = None
        self._inc_from: Optional[str] = None

    # incremental pushdown (chamado pelo main se disponível)
    def configure_incremental(self, *, field: str, from_date: str) -> None:
        self._inc_field = field
        self._inc_from = from_date

    def _compose_query(self) -> tuple[str, Dict[str, Any]]:
        q = self.query
        p = dict(self.params)

        if self._inc_field and self._inc_from:
            cond = f"{self._inc_field} >= :from_date"
            p["from_date"] = self._inc_from

            # 1) se o SQL trouxer um placeholder explícito, usamos ele
            if "{where_inc}" in q:
                q = q.replace("{where_inc}", cond)
            else:
                # 2) tentar injetar WHERE/AND simples
                qt = q.strip().lower()
                if " where " in qt:
                    q = f"{self.query} AND {cond}"
                else:
                    q = f"{self.query} WHERE {cond}"

        return q, p

    def extract(self) -> pd.DataFrame:
        engine = create_engine(self.source_url, future=True)
        q, p = self._compose_query()
        with engine.begin() as conn:
            df = pd.read_sql(text(q), conn, params=p)
        return df
