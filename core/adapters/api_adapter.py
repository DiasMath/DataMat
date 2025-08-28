from __future__ import annotations
import time
import logging
from typing import Any, Dict, List, Optional
import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import BoundedSemaphore

try:
    from core.auth.oauth2_client import OAuth2Client
except Exception:
    OAuth2Client = None  # type: ignore


class APISourceAdapter:
    """Adapter HTTP genérico (API → DataFrame) com estratégias de enriquecimento."""

    def __init__(
        self,
        endpoint: str,
        paging: Optional[Dict, Any] = None,
        timeout: Optional[int] = None,
        auth: Optional[Dict, Any] = None,
        default_headers: Optional[Dict, str] = None,
        params: Optional[Dict, Any] = None,
        enrich_by_id: bool = False,
        enrichment_strategy: str = 'concurrent',
        row_limit: Optional[int] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.paging = paging or {}
        self.timeout = timeout or int(os.getenv("API_TIMEOUT", "30"))
        self.auth_cfg = auth or {"kind": "none"}
        self.default_headers = dict(default_headers or {"Accept": "application/json"})
        self.base_params = dict(params or {})
        self.enrich_by_id = enrich_by_id
        self.enrichment_strategy = enrichment_strategy
        self.row_limit = row_limit

        self._session = requests.Session()
        self._max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
        self._backoff_base = float(os.getenv("API_BACKOFF_BASE", "0.5"))
        self._max_pages = int(os.getenv("API_MAX_PAGES", "1000"))
        
        self._detail_workers = int(os.getenv("API_DETAIL_WORKERS", "5"))
        self._concurrent_requests = int(os.getenv("API_CONCURRENT_REQUESTS", "3"))
        self._detail_delay_s = float(os.getenv("API_DETAIL_DELAY_S", "0.1"))
        self._rate_limit_semaphore = BoundedSemaphore(self._concurrent_requests)

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
                token_url=token_url or None, client_id=client_id or None,
                client_secret=client_secret or None, redirect_uri=redirect_uri or None,
                scope=scope, cache_path=cache_path, timeout=self.timeout,
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

    def _headers(self) -> Dict[str, str]:
        h = dict(self.default_headers)
        kind = (self.auth_cfg or {}).get("kind", "none")
        if kind == "none": return h
        if kind == "bearer_env":
            token = os.getenv(self.auth_cfg.get("env") or self.token_env or "", "")
            if not token: raise RuntimeError("bearer_env: variável de ambiente do token não definida.")
            h["Authorization"] = f"Bearer {token}"
            return h
        if kind == "header_token":
            token = os.getenv(self.auth_cfg.get("env") or "", "")
            header_name = self.auth_cfg.get("header") or "X-Api-Key"
            if not token: raise RuntimeError("header_token: variável de ambiente do token não definida.")
            h[header_name] = token
            return h
        if kind == "query_token": return h
        if kind == "oauth2_generic":
            if not self._oauth: raise RuntimeError("oauth2_generic: cliente OAuth2 não inicializado.")
            h["Authorization"] = f"Bearer {self._oauth.ensure_access_token()}"
            return h
        raise RuntimeError(f"Tipo de autenticação desconhecido: {kind}")

    def _request_with_retries(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        last_exc, headers = None, self._headers()
        for i in range(self._max_retries):
            try:
                r = self._session.get(url, headers=headers, params=params or {}, timeout=self.timeout)
                if r.status_code == 401 and self.auth_cfg.get("kind") == "oauth2_generic" and self._oauth:
                    self.log.warning("401 recebido. Tentando refresh de access_token ...")
                    self._oauth.refresh(); headers = self._headers()
                    r = self._session.get(url, headers=headers, params=params or {}, timeout=self.timeout)
                r.raise_for_status()
                return r
            except Exception as e:
                last_exc = e
                wait = self._backoff_base * (2 ** i)
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                    wait = float(e.response.headers.get("Retry-After", 5))
                    self.log.warning(f"Rate limit atingido (429). Esperando {wait:.1f}s ...")
                else:
                    self.log.warning("Tentativa %d falhou (%s). Retentando em %.1fs ...", i + 1, e.__class__.__name__, wait)
                time.sleep(wait)
        raise last_exc or RuntimeError("Falha desconhecida ao chamar a API")

    def _fetch_detail_concurrent(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self._rate_limit_semaphore:
            time.sleep(self._detail_delay_s)
            try:
                detail_resp = self._request_with_retries(f"{self.endpoint}/{item_id}")
                return self._row_from_detail_payload(detail_resp.json())
            except Exception as exc:
                self.log.error(f"   -> Falha ao buscar detalhes para o ID {item_id}: {exc}")
                return None

    def _first_page_params(self) -> Dict[str, Any]:
        params = dict(self.base_params)
        if self.paging.get("mode", "page") == "page":
            params[self.paging.get("page_param", "page")] = int(self.paging.get("start_page", 1))
            params[self.paging.get("size_param", "page_size")] = int(self.paging.get("size", 100))
        return params

    def _next_page_params(self, prev_json: Dict[str, Any], prev_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pconf = self.paging or {}
        if not pconf: return None
        if pconf.get("mode", "page") == "page":
            page_param, size_param = pconf.get("page_param", "page"), pconf.get("size_param", "page_size")
            size, page = int(pconf.get("size", 100)), int(prev_params.get(page_param, 1)) + 1
            if page > self._max_pages: return None
            nxt = dict(prev_params); nxt[page_param], nxt[size_param] = page, size
            return nxt
        return None

    def extract(self) -> pd.DataFrame:
        url, params, all_rows = self.endpoint, self._first_page_params(), []
        pg, t0 = 1, time.perf_counter()
        
        while True:
            if self.row_limit is not None and len(all_rows) >= self.row_limit:
                self.log.info(f" Límite de {self.row_limit} linhas atingido. Interrompendo paginação.")
                break

            self.log.info("🌐 GET %s | params=%s", url, params)
            resp = self._request_with_retries(url, params=params)
            js = resp.json()
            rows = self._rows_from_payload(js)
            if not rows:
                self.log.info("⛔ Página %d vazia. Encerrando paginação.", pg)
                break
            all_rows.extend(rows)
            self.log.info("📄 Página %d: %d linhas (acum: %d)", pg, len(rows), len(all_rows))
            
            params = self._next_page_params(js, params)
            if not params:
                break
            pg += 1

        if self.row_limit is not None:
            all_rows = all_rows[:self.row_limit]

        self.log.info("🧰 Total coletado na lista: %d linhas em %.2fs", len(all_rows), time.perf_counter() - t0)
        
        if not all_rows:
            return pd.DataFrame()
        
        df = pd.json_normalize(all_rows)
        
        if not self.enrich_by_id or df.empty or 'id' not in df.columns:
            return df

        ids_to_fetch = df['id'].tolist() # Processa todos os IDs, incluindo duplicatas
        enriched_rows, t_enrich = [], time.perf_counter()

        if self.enrichment_strategy == 'concurrent':
            self.log.info(f"🔎 Enriquecendo {len(ids_to_fetch)} registros com estratégia CONCORRENTE (max={self._concurrent_requests})...")
            with ThreadPoolExecutor(max_workers=self._detail_workers) as executor:
                results = executor.map(self._fetch_detail_concurrent, ids_to_fetch)
                enriched_rows = [row for row in results if row is not None]

        elif self.enrichment_strategy == 'sequential':
            self.log.info(f"🔎 Enriquecendo {len(ids_to_fetch)} registros com estratégia SEQUENCIAL (delay={self._detail_delay_s}s)...")
            for i, item_id in enumerate(ids_to_fetch):
                try:
                    time.sleep(self._detail_delay_s)
                    detail_resp = self._request_with_retries(f"{self.endpoint}/{item_id}")
                    detail_data = self._row_from_detail_payload(detail_resp.json())
                    enriched_rows.append(detail_data)
                    if (i + 1) % 100 == 0:
                        self.log.info(f"   ... {i+1}/{len(ids_to_fetch)} detalhes buscados")
                except Exception as exc:
                    self.log.error(f"   -> Falha ao buscar detalhes para o ID {item_id}: {exc}")
        else:
            raise ValueError(f"Estratégia de enriquecimento desconhecida: '{self.enrichment_strategy}'")

        if not enriched_rows:
            self.log.warning("Nenhum registro foi enriquecido com sucesso.")
            return df
        
        enriched_df = pd.json_normalize(enriched_rows)
        self.log.info("✅ Enriquecimento concluído: %d registros detalhados em %.2fs", len(enriched_df), time.perf_counter() - t_enrich)
        return enriched_df

    def _rows_from_payload(self, js: Any) -> List[Dict[str, Any]]:
        if isinstance(js, list): return js
        if isinstance(js, dict):
            for key in ("data", "items", "results"):
                val = js.get(key)
                if isinstance(val, list): return val
        return []

    def _row_from_detail_payload(self, js: Any) -> Dict[str, Any]:
        if isinstance(js, dict) and 'data' in js and isinstance(js['data'], dict):
            return js['data']
        return js