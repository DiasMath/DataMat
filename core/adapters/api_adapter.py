from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional
import requests
import pandas as pd
import os


class APISourceAdapter:
    def __init__(
        self,
        endpoint: str,
        token_env: Optional[str] = None,
        paging: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> None:
        self.endpoint = endpoint
        self.token_env = token_env
        self.paging = paging or {}
        self.timeout = timeout or int(os.getenv("API_TIMEOUT", "30"))

        self._session = requests.Session()
        self._max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
        self._backoff_base = float(os.getenv("API_BACKOFF_BASE", "0.5"))
        self._max_pages = int(os.getenv("API_MAX_PAGES", "1000"))

        # incremental (configurado pelo main quando disponível)
        self._inc_field: Optional[str] = None
        self._inc_from: Optional[str] = None

        # logger
        self.log = logging.getLogger(f"api_adapter:{self.endpoint}")
        if not self.log.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("%(message)s")
            h.setFormatter(fmt)
            self.log.addHandler(h)
        self.log.setLevel(logging.INFO)

    # ====== incremental pushdown ======
    def configure_incremental(self, *, field: str, from_date: str) -> None:
        """Recebe dica para já filtrar na origem usando exatamente o nome do campo fornecido."""
        self._inc_field = field
        self._inc_from = from_date
        self.log.info("📨 Incremental pushdown ativo: %s >= %s", field, from_date)

    # ====== headers ======
    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token_env:
            token = os.getenv(self.token_env)
            if token:
                h["Authorization"] = f"Bearer {token}"
        return h

    # ====== request com retry ======
    def _request_with_retries(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        last_exc: Optional[Exception] = None
        for i in range(self._max_retries):
            try:
                r = self._session.get(url, headers=self._headers(), params=params or {}, timeout=self.timeout)
                r.raise_for_status()
                return r
            except Exception as e:
                last_exc = e
                wait = self._backoff_base * (2 ** i)
                self.log.warning("Tentativa %d falhou (%s). Retentando em %.1fs ...", i + 1, e.__class__.__name__, wait)
                time.sleep(wait)
        raise last_exc or RuntimeError("Falha desconhecida ao chamar a API")

    # ====== paginação ======
    def _first_page_params(self) -> Dict[str, Any]:
        params = dict(self.paging.get("base_params", {}) or {})

        # incremental pushdown — usa exatamente o nome do campo informado
        if self._inc_field and self._inc_from:
            params[str(self._inc_field)] = str(self._inc_from)

        # paginação por número (default)
        if self.paging.get("mode", "page") == "page":
            params[self.paging.get("page_param", "page")] = int(self.paging.get("start_page", 1))
            params[self.paging.get("size_param", "page_size")] = int(self.paging.get("size", 200))
        return params

    def _next_page_params(self, prev_json: Dict[str, Any], prev_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pconf = self.paging
        if not pconf:
            return None

        # 1) paginação por número (page/page_size)
        if pconf.get("mode", "page") == "page":
            page_param = pconf.get("page_param", "page")
            size_param = pconf.get("size_param", "page_size")
            size = int(pconf.get("size", 200))
            page = int(prev_params.get(page_param, 1)) + 1
            if page > self._max_pages:
                return None
            nxt = dict(prev_params)
            nxt[page_param] = page
            nxt[size_param] = size
            return nxt

        # 2) paginação por cursor (next token no JSON)
        if pconf.get("mode") == "cursor":
            cursor_path = pconf.get("cursor_path", "next")
            cursor: Any = prev_json
            for part in cursor_path.split("."):
                cursor = cursor.get(part, {}) if isinstance(cursor, dict) else None
            if not cursor:
                return None
            nxt = dict(prev_params)
            nxt[pconf.get("cursor_param", "cursor")] = cursor
            return nxt

        return None

    # ====== extract ======
    def extract(self) -> pd.DataFrame:
        url = self.endpoint
        params = self._first_page_params()
        all_rows: List[Dict[str, Any]] = []

        # primeira requisição
        pg = 1
        t0 = time.perf_counter()
        self.log.info("🌐 GET %s | params=%s", url, params)
        resp = self._request_with_retries(url, params=params)
        js = resp.json()
        rows = self._rows_from_payload(js)
        all_rows.extend(rows)
        self.log.info("📄 Página %d: %d linhas (acum: %d)", pg, len(rows), len(all_rows))

        # próximas páginas
        nxt_params = self._next_page_params(js, params)
        while nxt_params:
            pg += 1
            self.log.info("🌐 GET %s | params=%s", url, nxt_params)
            resp = self._request_with_retries(url, params=nxt_params)
            js = resp.json()
            rows = self._rows_from_payload(js)
            if not rows:
                self.log.info("⛔ Página %d vazia. Encerrando paginação.", pg)
                break
            all_rows.extend(rows)
            self.log.info("📄 Página %d: %d linhas (acum: %d)", pg, len(rows), len(all_rows))
            nxt_params = self._next_page_params(js, nxt_params)

        self.log.info("🧰 Total coletado: %d linhas em %.2fs", len(all_rows), time.perf_counter() - t0)
        return pd.DataFrame(all_rows)

    # ====== extrair linhas do payload ======
    def _rows_from_payload(self, js: Any) -> List[Dict[str, Any]]:
        # tenta heuristicamente achar a lista
        if isinstance(js, list):
            return js  # lista de objetos
        if isinstance(js, dict):
            # caminhos comuns: data.items / data / results / items
            for key in ("items", "results", "data"):
                val = js.get(key)
                if isinstance(val, list):
                    return val
                if isinstance(val, dict):
                    inner = val.get("items") or val.get("results")
                    if isinstance(inner, list):
                        return inner
        # fallback
        return []
