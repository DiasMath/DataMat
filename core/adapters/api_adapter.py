from __future__ import annotations
import logging
import time
import os
import json
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock
import itertools

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
        if self.period_seconds == 0.0: 
            return
        with self.lock:
            wait_time = self.period_seconds - (time.monotonic() - self.last_request_time)
            if wait_time > 0:
                time.sleep(wait_time)
            self.last_request_time = time.monotonic()


class APISourceAdapter:
    """
    Adapter para extrair dados de uma fonte API HTTP.
    Combina uma matriz de parâmetros para extração completa com um laço de 
    consolidação para máxima resiliência contra inconsistências da API.
    """
    def __init__(
        self,
        endpoint: str,
        base_url_env_var: str = "API_BASE_URL",
        paging: Optional[Dict] = None,
        auth: Optional[Dict] = None,
        params: Optional[Dict] = None,
        param_matrix: Optional[Dict] = None,
        enrich_by_id: bool = False,
        enrichment_strategy: str = 'concurrent',
        row_limit: Optional[int] = None,
        data_path: Optional[str] = None,
        detail_data_path: Optional[str] = None,
        requests_per_minute: Optional[int] = 60,
        enrichment_requests_per_minute: Optional[int] = None,
        delay_between_pages_ms: Optional[int] = None,
        max_passes: int = 1
    ):
        self.paging = paging or {}
        self.base_params = params or {}
        self.param_matrix = param_matrix or {}
        self.enrich_by_id = enrich_by_id
        self.row_limit = row_limit
        self.data_path = data_path
        self.detail_data_path = detail_data_path
        self.enrichment_strategy = enrichment_strategy
        self.delay_between_pages_ms = delay_between_pages_ms
        self.max_passes = max_passes

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

    def _execute_full_pass(self) -> List[Dict]:
        """
        Executa uma passagem completa, iterando pela `param_matrix` se ela existir.
        Retorna uma lista de todos os registros brutos encontrados na passagem.
        """
        if self.param_matrix:
            keys, values = self.param_matrix.keys(), self.param_matrix.values()
            param_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
            if len(param_combinations) > 1:
                log.info(f"Executando {len(param_combinations)} combinações de parâmetros.")
        else:
            param_combinations = [{}]

        all_rows_for_pass = []
        for i, combo in enumerate(param_combinations):
            current_params = self.base_params.copy()
            current_params.update(combo)

            if len(param_combinations) > 1:
                log.info(f"--- Conjunto {i+1}/{len(param_combinations)}: {combo} ---")
            
            pass_rows = self._fetch_all_pages(params_override=current_params)
            all_rows_for_pass.extend(pass_rows)
        
        return all_rows_for_pass

    def extract_raw(self) -> List[Dict]:
        """
        Método principal de extração. 
        Se max_passes=1 (padrão), executa apenas uma extração linear.
        Se max_passes>1, executa loops de consolidação para APIs instáveis.
        """
        consolidated_data = {} 
        
        # Usa o parâmetro configurável
        for i in range(1, self.max_passes + 1):
            
            # Só loga "Passagem X/Y" se realmente houver múltiplas passagens configuradas
            if self.max_passes > 1:
                log.info(f"--- Iniciando Passagem de Extração Completa nº {i}/{self.max_passes} ---")
            
            unique_records_before = len(consolidated_data)

            raw_rows_pass = self._execute_full_pass()

            if not raw_rows_pass:
                if self.max_passes > 1:
                    log.warning(f"Passagem {i} não retornou nenhum dado novo.")
                
                if i > 1:
                    break
                else:
                    # Se falhou na primeira e única passagem, retorna vazio
                    return []
            
            # Consolidação
            for record in raw_rows_pass:
                if isinstance(record, dict) and 'id' in record:
                    consolidated_data[record['id']] = record
                else:
                    # Se não tem ID, confia na lista bruta (para casos sem deduplicação por ID)
                     if self.max_passes == 1:
                        return raw_rows_pass

            # Se for passagem única, retorna direto sem lógica de estabilização
            if self.max_passes == 1:
                final_list = list(consolidated_data.values())
                if self.enrich_by_id:
                    final_list = self._enrich_data_raw(final_list)
                return final_list

            unique_records_after = len(consolidated_data)
            log.info(f"--- Fim da passagem {i}: {unique_records_after} registros consolidados. ---")
            
            if unique_records_after == unique_records_before and i > 1:
                log.info("✅ Extração estabilizada. Finalizando consolidação.")
                break
            
            if i < self.max_passes and i >= 1:
                log.info("Aguardando 5s antes da próxima passagem de consolidação...")
                time.sleep(5)
        
        final_list = list(consolidated_data.values())

        if self.enrich_by_id:
            final_list = self._enrich_data_raw(final_list)
            
        return final_list

    def _fetch_all_pages(self, params_override: Optional[Dict] = None) -> List[Dict]:
        """
        Executa a paginação para um único conjunto de parâmetros.
        """
        all_rows: List[Dict] = []
        effective_params = params_override if params_override is not None else self.base_params.copy()
        
        params = self._get_first_page_params(base_params=effective_params)
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
                # O _get_data_from_payload agora garante uma lista, mas mantemos por segurança.
                log.error(f"⛔ Formato de dados inesperado (não é uma lista). Encerrando. Payload: {payload}")
                break

            num_records = len(page_data)
            all_rows.extend(page_data)
            log.info(f"   -> Recebidos: {num_records} registos | Total acumulado na passagem: {len(all_rows)}")
            
            if num_records == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    log.info(f"✅ Encontradas {consecutive_empty_pages} páginas vazias em sequência. Assumindo fim dos dados.")
                    break
            else:
                consecutive_empty_pages = 0

            if num_records < page_size:
                log.info(f"✅ Última página detetada ({num_records} de {page_size} registos). Encerrando paginação.")
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
                # _get_data_from_payload retorna uma lista, pegamos o primeiro elemento
                enriched_list = self._get_data_from_payload(response.json(), self.detail_data_path)
                return enriched_list[0] if enriched_list else None
            except Exception as e:
                log.error(f"Falha ao enriquecer ID {item_id}: {e}")
                return None

    def _enrich_data_raw(self, raw_data: List[Dict]) -> List[Dict]:
        """Versão do enriquecimento que opera sobre a lista de dicionários brutos."""
        if not raw_data or 'id' not in raw_data[0]:
            raise DataExtractionError("Dados brutos inválidos ou sem 'id' para enriquecimento.")
        
        ids_to_fetch = list(set(item['id'] for item in raw_data if 'id' in item))

        if not ids_to_fetch:
            log.warning("Nenhum ID único encontrado para enriquecer.")
            return raw_data
            
        log.info(f"🔎 Enriquecendo {len(ids_to_fetch)} registos com estratégia '{self.enrichment_strategy}'...")
        enriched_rows: List[Dict] = []
        if self.enrichment_strategy == 'concurrent':
            with ThreadPoolExecutor(max_workers=self._detail_workers) as executor:
                results = executor.map(self._enrich_one_detail, ids_to_fetch)
                enriched_rows = [row for row in results if row is not None]
        else: # sequential
            for i, item_id in enumerate(ids_to_fetch):
                if (i + 1) % 50 == 0: 
                    log.info(f"   ... {i+1}/{len(ids_to_fetch)} detalhes buscados")
                detail = self._enrich_one_detail(item_id)
                if detail: enriched_rows.append(detail)
        
        if not enriched_rows:
            log.warning("Nenhum registo foi enriquecido com sucesso.")
            return raw_data # Retorna os dados originais se o enriquecimento falhar
            
        return enriched_rows
    
    def _get_first_page_params(self, base_params: Dict) -> Dict:
        params = base_params.copy()
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
        """
        Extrai os dados do payload de forma robusta. 
        Se o resultado final for um único dicionário, ele é encapsulado em uma 
        lista para manter a consistência do fluxo de dados.
        """
        data = payload
        
        if path:
            try:
                for key in path.split('.'):
                    if isinstance(data, list): data = data[0] if data else {}
                    data = data[key]
            except (KeyError, TypeError, IndexError):
                log.warning(f"Caminho de dados '{path}' não encontrado no payload: {payload}")
                return []
        
        if isinstance(data, dict):
            for key in ("data", "items", "results", "content"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]

        if isinstance(data, list):
            return data

        log.warning(f"Payload não pôde ser processado em uma lista de registros. Payload: {payload}")
        return []