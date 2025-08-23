from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional
import requests
import pandas as pd
import os

try:
    from core.auth.oauth2_client import OAuth2Client
except Exception:
    OAuth2Client = None  # type: ignore


class APISourceAdapter:
    """
    Adapter HTTP genérico (API → DataFrame).

    Autenticação por job (self.auth_cfg):
      - {"kind": "none"}
      - {"kind": "bearer_env", "env": "API_TOKEN"}
      - {"kind": "header_token", "env": "API_KEY", "header": "X-Api-Key"}
      - {"kind": "query_token", "env": "API_KEY", "param": "apikey"}
      - {"kind": "oauth2_generic",
         "token_cache": ".secrets/oauth_tokens.json",
         "token_url_env": "OAUTH_TOKEN_URL",
         "client_id_env": "OAUTH_CLIENT_ID",
         "client_secret_env": "OAUTH_CLIENT_SECRET",
         "redirect_uri_env": "OAUTH_REDIRECT_URI",
         "scope_env": "OAUTH_SCOPE",
         "auth_code_env": "OAUTH_AUTH_CODE",
         "use_basic_auth": True}

    Paginação:
      - page: {"mode":"page","page_param":"page","size_param":"page_size","size":200,"start_page":1}
      - cursor: {"mode":"cursor","cursor_path":"next","cursor_param":"cursor"}
    """

    def __init__(
        self,
        endpoint: str,
        token_env: Optional[str] = None,          # legado
        paging: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        auth: Optional[Dict[str, Any]] = None,    # <<< novo
        default_headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.token_env = token_env
        self.paging = paging or {}
        self.timeout = timeout or int(os.getenv("API_TIMEOUT", "30"))
        self.auth_cfg = auth or ({"kind": "bearer_env", "env": token_env} if token_env else {"kind": "none"})
        self.default_headers = dict(default_headers or {"Accept": "application/json"})
        self.base_params = dict(params or {})

        self._session = requests.Session()
        self._max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
        self._backoff_base = float(os.getenv("API_BACKOFF_BASE", "0.5"))
        self._max_pages = int(os.getenv("API_MAX_PAGES", "1000"))

        self._inc_field: Optional[str] = None
        self._inc_from: Optional[str] = None

        self._oauth: Optional[OAuth2Client] = None
        if self.auth_cfg.get("kind") == "oauth2_generic":
            if OAuth2Client is None:
                raise RuntimeError("oauth2_generic: OAuth2Client indisponível.")
            token_url = os.getenv(self.auth_cfg.get("token_url_env", "OAUTH_TOKEN_URL"), "")
            client_id = os.getenv(self.auth_cfg.get("client_id_env", "OAUTH_CLIENT_ID"), "")
            client_secret = os.getenv(self.auth_cfg.get("client_secret_env", "OAUTH_CLIENT_SECRET"), "")
            redirect_uri = os.getenv(self.auth_cfg.get("redirect_uri_env", "OAUTH_REDIRECT_URI"), "")
            scope = os.getenv(self.auth_cfg.get("scope_env", "OAUTH_SCOPE"), None)
            cache_path = self.auth_cfg.get("token_cache", ".secrets/oauth_tokens.json")
            use_basic = bool(self.auth_cfg.get("use_basic_auth", True))
            self._oauth = OAuth2Client(
                token_url=token_url or None,
                client_id=client_id or None,
                client_secret=client_secret or None,
                redirect_uri=redirect_uri or None,
                scope=scope,
                cache_path=cache_path,
                timeout=self.timeout,
                use_basic_auth=use_basic,
            )
            if not getattr(self._oauth, "_tokens", {}).get("access_token"):
                auth_code = os.getenv(self.auth_cfg.get("auth_code_env", "OAUTH_AUTH_CODE"), "")
                self._oauth.exchange_code(auth_code)

        self.log = logging.getLogger(f"api_adapter:{self.endpoint}")
        if not self.log.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("%(message)s"))
            self.log.addHandler(h)
        self.log.setLevel(logging.INFO)

    # incremental pushdown
    def configure_incremental(self, *, field: str, from_date: str) -> None:
        self._inc_field = field
        self._inc_from = from_date
        self.log.info("📨 Incremental pushdown ativo: %s >= %s", field, from_date)

    # headers por estratégia
    def _headers(self) -> Dict[str, str]:
        h = dict(self.default_headers)
        kind = (self.auth_cfg or {}).get("kind", "none")

        if kind == "none":
            return h

        if kind == "bearer_env":
            token_name = self.auth_cfg.get("env") or self.token_env
            token = os.getenv(token_name or "", "")
            if not token:
                raise RuntimeError("bearer_env: variável de ambiente do token não definida.")
            h["Authorization"] = f"Bearer {token}"
            return h

        if kind == "header_token":
            token = os.getenv(self.auth_cfg.get("env") or "", "")
            header_name = self.auth_cfg.get("header") or "X-Api-Key"
            if not token:
                raise RuntimeError("header_token: variável de ambiente do token não definida.")
            h[header_name] = token
            return h

        if kind == "query_token":
            return h  # param entra em _first_page_params

        if kind == "oauth2_generic":
            if not self._oauth:
                raise RuntimeError("oauth2_generic: cliente OAuth2 não inicializado.")
            h["Authorization"] = f"Bearer {self._oauth.ensure_access_token()}"
            return h

        raise RuntimeError(f"Tipo de autenticação desconhecido: {kind}")

    # params iniciais
    def _first_page_params(self) -> Dict[str, Any]:
        params = dict(self.base_params)
        if self._inc_field and self._inc_from:
            params[str(self._inc_field)] = str(self._inc_from)
        if self.auth_cfg.get("kind") == "query_token":
            token = os.getenv(self.auth_cfg.get("env") or "", "")
            if not token:
                raise RuntimeError("query_token: variável de ambiente do token não definida.")
            params[self.auth_cfg.get("param", "apikey")] = token
        if self.paging.get("mode", "page") == "page":
            params[self.paging.get("page_param", "page")] = int(self.paging.get("start_page", 1))
            params[self.paging.get("size_param", "page_size")] = int(self.paging.get("size", 200))
        return params

    # próxima página
    def _next_page_params(self, prev_json: Dict[str, Any], prev_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pconf = self.paging or {}
        if not pconf:
            return None
        if pconf.get("mode", "page") == "page":
            page_param = pconf.get("page_param", "page")
            size_param = pconf.get("size_param", "page_size")
            size = int(pconf.get("size", 200))
            page = int(prev_params.get(page_param, 1)) + 1
            if page > self._max_pages:
                return None
            nxt = dict(prev_params); nxt[page_param] = page; nxt[size_param] = size
            return nxt
        if pconf.get("mode") == "cursor":
            cursor_path = pconf.get("cursor_path", "next")
            cursor: Any = prev_json
            for part in cursor_path.split("."):
                cursor = cursor.get(part, {}) if isinstance(cursor, dict) else None
            if not cursor:
                return None
            nxt = dict(prev_params); nxt[pconf.get("cursor_param", "cursor")] = cursor
            return nxt
        return None

    # GET com retries (e refresh em 401 para OAuth2)
    def _request_with_retries(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        last_exc: Optional[Exception] = None
        headers = self._headers()

        for i in range(self._max_retries):
            try:
                r = self._session.get(url, headers=headers, params=params or {}, timeout=self.timeout)
                if r.status_code == 401 and self.auth_cfg.get("kind") == "oauth2_generic" and self._oauth:
                    self.log.warning("401 recebido. Tentando refresh de access_token ...")
                    self._oauth.refresh()
                    headers = self._headers()
                    r = self._session.get(url, headers=headers, params=params or {}, timeout=self.timeout)
                r.raise_for_status()
                return r
            except Exception as e:
                last_exc = e
                wait = self._backoff_base * (2 ** i)
                self.log.warning("Tentativa %d falhou (%s). Retentando em %.1fs ...", i + 1, e.__class__.__name__, wait)
                time.sleep(wait)
        raise last_exc or RuntimeError("Falha desconhecida ao chamar a API")

    # extract
    def extract(self) -> pd.DataFrame:
        url = self.endpoint
        params = self._first_page_params()
        all_rows: List[Dict[str, Any]] = []

        pg = 1
        t0 = time.perf_counter()
        self.log.info("🌐 GET %s | params=%s", url, params)
        resp = self._request_with_retries(url, params=params)
        js = resp.json()
        rows = self._rows_from_payload(js)
        all_rows.extend(rows)
        self.log.info("📄 Página %d: %d linhas (acum: %d)", pg, len(rows), len(all_rows))

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

    # heurística p/ achar lista de itens
    def _rows_from_payload(self, js: Any) -> List[Dict[str, Any]]:
        if isinstance(js, list):
            return js
        if isinstance(js, dict):
            for key in ("items", "results", "data"):
                val = js.get(key)
                if isinstance(val, list):
                    return val
                if isinstance(val, dict):
                    inner = val.get("items") or val.get("results")
                    if isinstance(inner, list):
                        return inner
        return []
