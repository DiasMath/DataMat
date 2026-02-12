from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class Job:
    """
    Define a estrutura de um Job de extração para a Staging Area.
    Cada instância desta classe representa uma fonte de dados a ser processada.
    """
    
     # --- Identificação Mapeamento ---
    name: str
    type: str
    map_id: str

    # --- Configuração de Destino ---
    db_name: str
    table: str

    # --- Parâmetros gerais ---
    params: Optional[Dict[str, Any]] = None
    param_matrix: Optional[Dict[str, List[Any]]] = None
    param_sequence: Optional[List[Dict[str, Any]]] = None
    incremental_config: Optional[Dict[str, Any]] = None  # Sobrepõe se colocar data no param_matrix.
        # Exemplo: {
        #    "enabled": True, 
        #    "days_to_load": 7, 
        #    "date_param_start": "dataInicial", 
        #    "date_param_end": "dataFinal",
        #    "date_filter_field": "tipoFiltro",  # Opcional (solicitado)
        #    "date_filter_value": "V"            # Opcional (solicitado)
        # }
    full_load_config: Optional[Dict[str, Any]] = None
        # Exemplo: {
        #     "weekday": 6, # 0=Seg, 6=Dom
        #     "truncate": True,
        #     "years": [2025, 2026],
        #     "date_param_start": "dataEmissaoInicial",
        #     "date_param_end": "dataEmissaoFinal",
        #     "date_filter_param": "tipoData", # Opcional
        #     "date_filter_value": "V"         # Opcional
        # },
    merge_mode: str = "legacy"  # "legacy" (padrão, seguro) ou "iodku" (rápido, exige Unique Key)

    # --- Parâmetros Específicos por Tipo de Adapter ---
    # API
    endpoint: Optional[str] = None
    auth: Optional[Dict[str, Any]] = None
    paging: Optional[Dict[str, Any]] = None
    data_path: Optional[str] = None
    detail_data_path: Optional[str] = None
    enrich_by_id: bool = False
    enrichment_strategy: str = 'sequential'  # Estratégia: 'concurrent' ou 'sequential'
    requests_per_minute: Optional[int] = 60  # Limite de requisições para a extração principal
    enrichment_requests_per_minute: Optional[int] = None # Limite para o enriquecimento (se None, usa o principal)
    delay_between_pages_ms: Optional[int] = None
    max_passes: int = 1 # Desconfiar da API e refazer requisições 
    truncate: bool = False  # Flag manual caso você queira forçar na mão
    full_load_weekday: Optional[int] = None # Agendamento Automático (0=Seg, 6=Dom). Ex: 6 para rodar Full todo domingo.
    id_key: str = "id"  # Define o padrão como "id", mas permite mudar para "sku", "uuid", etc.

    # Arquivo (File)
    file: Optional[str] = None
    sheet: Optional[str] = None
    delimiter: Optional[str] = ';'

    # Banco de Dados (DB)
    source_url: Optional[str] = None
    query: Optional[str] = None