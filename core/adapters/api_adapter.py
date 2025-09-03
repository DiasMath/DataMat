from __future__ import annotations
import logging
import time
import os
import json
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock

import requests
import pandas as pd

from core.errors.exceptions import DataExtractionError
from core.auth.oauth2_client import OAuth2Client

log = logging.getLogger(__name__)


class _RateLimiter:
    """Implementa um controlo de taxa de requisições."""
    def __init__(self, requests_per_minute: int | None):
        if requests_per_minute and requests_per_minute > 0:
            self.period_seconds = 60.0 / requests_per_minute
        else:
            self.period_seconds = 0.0
        self.lock = Lock()
        self.last_request_time = 0.0

    def wait(self):
        if self.period_seconds == 0.0: return
        with self.lock:
            wait_time = self.period_seconds - (time.monotonic() - self.last_request_time)
            if wait_time > 0:
                time.sleep(wait_time)
            self.last_request_time = time.monotonic()


class APISourceAdapter:
    """
    Adapter para extrair dados de uma fonte API HTTP.
    Implementa uma "Estratégia Adaptativa": executa de forma rápida para APIs
    estáveis e ativa um modo de consolidação robusto para APIs instáveis.
    """
    def __init__(
        self,
        endpoint: str,
        base_url_env_var: str = "API_BASE_URL",
        paging: Optional[Dict] = None,
        auth: Optional[Dict] = None,
        params: Optional[Dict] = None,
        enrich_by_id: bool = False,
        enrichment_strategy: str = 'concurrent',
        row_limit: Optional[int] = None,
        data_path: Optional[str] = None,
        detail_data_path: Optional[str] = None,
        requests_per_minute: Optional[int] = 60,
        enrichment_requests_per_minute: Optional[int] = None,
        delay_between_pages_ms: Optional[int] = None
    ):
        self.paging = paging or {}
        self.base_params = params or self.paging.get("params", {})
        self.enrich_by_id = enrich_by_id
        self.row_limit = row_limit
        self.data_path = data_path
        self.detail_data_path = detail_data_path
        self.enrichment_strategy = enrichment_strategy
        self.delay_between_pages_ms = delay_between_pages_ms

        self._session = requests.Session()
        self._max_retries = int(os.getenv("API_MAX_RETRIES", "3"))
        self._backoff_base = float(os.getenv("API_BACKOFF_BASE", "0.5"))
        self._timeout = int(os.getenv("API_TIMEOUT", "30"))
        self.retryable_status_codes: Set[int] = {429, 500, 502, 503, 504}
        
        base_url = os.getenv(base_url_env_var)
        if not base_url:
            raise ValueError(f"A variável de ambiente '{base_url_env_var}' não está definida.")
        self.full_endpoint_url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        self._auth_config = auth or {}
        self._oauth_client: OAuth2Client | None = None
        if self._auth_config.get("kind") == "oauth2_generic":
            self._oauth_client = self._setup_oauth_client()
        
        self._main_rate_limiter = _RateLimiter(requests_per_minute)
        rpm_enrich = enrichment_requests_per_minute or requests_per_minute
        self._enrich_rate_limiter = _RateLimiter(rpm_enrich)

        self._detail_workers = int(os.getenv("API_DETAIL_WORKERS", "5"))
        self._concurrent_requests = int(os.getenv("API_CONCURRENT_REQUESTS", "3"))
        self._enrich_semaphore = BoundedSemaphore(self._concurrent_requests)

    def _setup_oauth_client(self) -> OAuth2Client:
        log.info("Configurando cliente OAuth2 para o APIAdapter...")
        config = self._auth_config
        client_id_var = config.get("client_id_env", "OAUTH_CLIENT_ID")
        client_id = os.getenv(client_id_var)
        if not client_id:
             raise ValueError(f"Variável de ambiente '{client_id_var}' não definida para OAuth2.")
        token_cache_path = f".secrets/bling_tokens_{client_id}.json"
        return OAuth2Client(
            token_url=os.getenv(config.get("token_url_env", "OAUTH_TOKEN_URL")),
            client_id=client_id,
            client_secret=os.getenv(config.get("client_secret_env", "OAUTH_CLIENT_SECRET")),
            redirect_uri=os.getenv(config.get("redirect_uri_env", "OAUTH_REDIRECT_URI")),
            scope=os.getenv(config.get("scope_env", "OAUTH_SCOPE")),
            cache_path=token_cache_path
        )

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        kind = self._auth_config.get("kind", "none")
        if kind == "oauth2_generic":
            if not self._oauth_client:
                raise RuntimeError("Cliente OAuth2 foi configurado mas não inicializado.")
            access_token = self._oauth_client.ensure_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
        return headers

    def _make_request(self, url: str, params: Dict, rate_limiter: _RateLimiter) -> requests.Response:
        last_exc = None
        for i in range(self._max_retries):
            rate_limiter.wait()
            try:
                response = self._session.get(url, headers=self._get_headers(), params=params, timeout=self._timeout)
                if response.status_code == 401 and self._oauth_client:
                    log.warning("Recebido status 401. Forçando refresh do token e tentando novamente...")
                    self._oauth_client.refresh()
                    response = self._session.get(url, headers=self._get_headers(), params=params, timeout=self._timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                last_exc = e
                log.warning(f"Tentativa {i+1} falhou: {e}. Retentando...")
                time.sleep(self._backoff_base * (2 ** i))
        raise DataExtractionError("Máximo de retentativas atingido.") from last_exc

    def _fetch_all_pages(self) -> List[Dict]:
        """
        Executa uma única "varredura" completa de todas as páginas da API.
        """
        all_rows: List[Dict] = []
        params = self._get_first_page_params()
        page_size = params.get(self.paging.get("size_param", "limite"), 100)
        
        max_pages = int(os.getenv("API_MAX_PAGES", "1000"))
        consecutive_empty_pages = 0
        
        for page_num in range(1, max_pages + 1):
            if self.row_limit and len(all_rows) >= self.row_limit:
                log.info(f"Limite de {self.row_limit} linhas atingido. Encerrando.")
                break
            
            log.info(f"📄 Página {page_num}: GET {self.full_endpoint_url} | params={params}")

            try:
                response = self._make_request(self.full_endpoint_url, params, self._main_rate_limiter)
                payload = response.json()
            except Exception as e:
                error_payload = getattr(getattr(e, 'response', None), 'text', '{}')
                log.error(f"Falha crítica ao buscar a página {page_num}: {e}. Corpo: {error_payload}", exc_info=False)
                raise DataExtractionError(f"Falha na requisição da página {page_num}") from e
            
            page_data = self._get_data_from_payload(payload, self.data_path)
            
            if not isinstance(page_data, list):
                log.error(f"⛔ Formato de dados inesperado (não é uma lista). Encerrando. Payload: {payload}")
                break

            num_records = len(page_data)
            all_rows.extend(page_data)
            log.info(f"   -> Recebidos: {num_records} registos | Total acumulado na passagem: {len(all_rows)}")
            
            if num_records == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    log.info(f"✅ Encontradas {consecutive_empty_pages} páginas vazias em sequência. Assumindo fim dos dados para esta passagem.")
                    break
            else:
                consecutive_empty_pages = 0

            if num_records < page_size:
                log.info(f"✅ Última página detetada ({num_records} de {page_size} registos). Encerrando paginação para esta passagem.")
                break

            params = self._get_next_page_params(params)
            if not params: break
            
            if self.delay_between_pages_ms and self.delay_between_pages_ms > 0:
                delay_seconds = self.delay_between_pages_ms / 1000
                log.info(f"   -> Aguardando {delay_seconds:.2f}s antes da próxima página...")
                time.sleep(delay_seconds)
                
        return all_rows

    def _enrich_one_detail(self, item_id: Any) -> Optional[Dict]:
        with self._enrich_semaphore:
            try:
                detail_url = f"{self.full_endpoint_url}/{item_id}"
                response = self._make_request(detail_url, params={}, rate_limiter=self._enrich_rate_limiter)
                return self._get_data_from_payload(response.json(), self.detail_data_path)
            except Exception as e:
                log.error(f"Falha ao enriquecer ID {item_id}: {e}")
                return None

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'id' not in df.columns:
            raise DataExtractionError("Coluna 'id' não encontrada para enriquecimento.")
        ids_to_fetch = df['id'].dropna().unique().tolist()
        if not ids_to_fetch:
            log.warning("Nenhum ID único encontrado para enriquecer.")
            return df
        log.info(f"🔎 Enriquecendo {len(ids_to_fetch)} registos com estratégia '{self.enrichment_strategy}'...")
        enriched_rows: List[Dict] = []
        if self.enrichment_strategy == 'concurrent':
            with ThreadPoolExecutor(max_workers=self._detail_workers) as executor:
                results = executor.map(self._enrich_one_detail, ids_to_fetch)
                enriched_rows = [row for row in results if row is not None]
        else: # sequential
            for i, item_id in enumerate(ids_to_fetch):
                if (i + 1) % 50 == 0: log.info(f"   ... {i+1}/{len(ids_to_fetch)} detalhes buscados")
                detail = self._enrich_one_detail(item_id)
                if detail: enriched_rows.append(detail)
        if not enriched_rows:
            log.warning("Nenhum registo foi enriquecido com sucesso.")
            return df
        return pd.json_normalize(enriched_rows)

    def extract(self) -> pd.DataFrame:
        """
        REFATORADO com a "Estratégia Adaptativa":
        1. Executa uma primeira passagem de extração.
        2. Verifica se há duplicatas.
        3. Se houver, ativa o modo de consolidação para garantir a completude.
           Caso contrário, finaliza rapidamente.
        """
        log.info("--- Iniciando Passagem de Extração nº 1 (Teste de Confiança) ---")
        all_rows = self._fetch_all_pages()
        if not all_rows:
            return pd.DataFrame()
            
        consolidated_df = pd.json_normalize(all_rows)

        # A verificação de integridade
        if 'id' in consolidated_df.columns and consolidated_df.duplicated(subset=['id']).any():
            log.warning("🚨 API instável detectada! Ativando modo de consolidação para garantir a captura de todos os dados.")
            
            # A partir daqui, a lógica de consolidação é ativada.
            max_passes = 5
            for i in range(2, max_passes + 1): # Começa da passagem 2
                log.info(f"--- Iniciando Passagem de Extração nº {i}/{max_passes} ---")
                
                unique_rows_before = consolidated_df.drop_duplicates(subset=['id']).shape[0]

                raw_rows_pass = self._fetch_all_pages()
                if not raw_rows_pass:
                    log.warning(f"Passagem {i} não retornou nenhum dado novo.")
                    continue
                
                pass_df = pd.json_normalize(raw_rows_pass)
                if not pass_df.empty:
                    consolidated_df = pd.concat([consolidated_df, pass_df], ignore_index=True)
                
                consolidated_df = consolidated_df.drop_duplicates(subset=['id'], keep='last')
                unique_rows_after = consolidated_df.shape[0]

                log.info(f"--- Fim da passagem {i}: {unique_rows_after} registros únicos consolidados. (+{unique_rows_after - unique_rows_before} novos) ---")
                
                if unique_rows_after == unique_rows_before:
                    log.info("✅ Extração estabilizada. Finalizando consolidação.")
                    break
                
                if i < max_passes:
                    log.info("Aguardando 5s antes da próxima passagem de consolidação...")
                    time.sleep(5)
        else:
            log.info("✅ API estável. Nenhuma duplicata encontrada na primeira passagem. Finalizando extração.")
            # Se não houver duplicatas, apenas garante que o resultado final está limpo.
            if 'id' in consolidated_df.columns:
                 consolidated_df = consolidated_df.drop_duplicates(subset=['id'], keep='last')

        if self.enrich_by_id:
            consolidated_df = self._enrich_data(consolidated_df)
            
        return consolidated_df
    
    def _get_first_page_params(self) -> Dict:
        params = self.base_params.copy()
        if self.paging.get("mode") == "page":
            params[self.paging.get("page_param", "page")] = self.paging.get("start_page", 1)
            params[self.paging.get("size_param", "limite")] = self.paging.get("size", 100)
        return params

    def _get_next_page_params(self, current_params: Dict) -> Optional[Dict]:
        if self.paging.get("mode") == "page":
            next_params = current_params.copy()
            page_param = self.paging.get("page_param", "page")
            next_params[page_param] = current_params.get(page_param, 1) + 1
            return next_params
        return None

    def _get_data_from_payload(self, payload: Any, path: Optional[str]) -> Any:
        if not path:
            if isinstance(payload, list): 
                return payload
            if isinstance(payload, dict):
                for key in ("data", "items", "results", "content"):
                    if isinstance(payload.get(key), list):
                        return payload[key]
                return payload
            return []
        
        data = payload
        try:
            for key in path.split('.'):
                if isinstance(data, list): data = data[0] if data else {}
                data = data[key]
            return data
        except (KeyError, TypeError, IndexError):
            log.warning(f"Caminho de dados '{path}' não encontrado no payload.")
            return []