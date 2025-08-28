from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import os

@dataclass
class Job:
    name: str
    type: str                      # "api" | "file" | "db"
    db_name: Optional[str]         # "DB_STG_NAME" (chave do .env) ou nome literal (usado na {db} do DB_URL)
    table: str                     # nome da tabela destino
    map_id: Optional[str] = None   # id do mapping no mappings.py
    params: Optional[Dict[str, Any]] = None # Parametros adicionais, serve para API e DB->DB

    # ---- origem (API) ----
    endpoint: Optional[str] = None
    auth: Optional[Dict[str, Any]] = None # Configuração de autenticação
    paging: Optional[Dict[str, Any]] = None   # Manter só se a API tiver paginação REAL
    enrich_by_id: bool = False          # Flag para ativar o enriquecimento
    enrichment_strategy: str = 'concurrent'
    timeout: Optional[int] = None

    # ---- arquivo (se usar) ----
    file: Optional[str] = None
    sheet: Optional[str] = None
    header: Optional[int] = None

    # ---- db->db (se usar) ----
    source_url: Optional[str] = None
    query: Optional[str] = None

# ====================================================================
#  CONFIGURAÇÕES REUTILIZÁVEIS
# ====================================================================

# Centraliza a configuração de autenticação OAuth2 do Bling UMA VEZ.
BLING_OAUTH_CONFIG: Dict[str, Any] = {
    "kind": "oauth2_generic",
    "token_cache": f".secrets/bling_tokens_{os.getenv('CLIENT_ID')}.json",
    "token_url_env": "OAUTH_TOKEN_URL",
    "client_id_env": "OAUTH_CLIENT_ID",
    "client_secret_env": "OAUTH_CLIENT_SECRET",
    "redirect_uri_env": "OAUTH_REDIRECT_URI",
}

PAGING: Dict[str, Any] = {
    "mode": "page",
    "page_param": "pagina",
    "size_param": "limite",
    "size": 100,
}

# ====================================================================
#  LISTA DE JOBS DE EXTRAÇÃO
# ====================================================================

JOBS: List[Job] = [
    # ===================== BLING API JOBS =====================#

    # ========= DRE ========= #
    Job(
        name="Carga STG Bling Categorias Financeiras",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_CategoriasFinanceiras",
        map_id="map_bling_categorias_financeiras",
        endpoint="categorias/receitas-despesas",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Contas a Pagar",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasPagar",
        map_id="map_bling_contas_pagar",
        endpoint="contas/pagar",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        params={
            "dataEmissaoInicial": "2022-01-01",
            "dataEmissaoFinal": "2022-12-31",
        }
        
    ),

    Job(
        name="Carga STG Bling Contas a Receber",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasReceber",
        map_id="map_bling_contas_receber",
        endpoint="contas/receber",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Contas Contabeis",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasContabeis",
        map_id="map_bling_contas_contabeis",
        endpoint="contas-contabeis",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Formas Pagamento",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_FormasPagamento",
        map_id="map_bling_formas_pagamento",
        endpoint="formas-pagamentos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Nota Fiscal",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscal",
        map_id="map_bling_nota_fiscal",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Item",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalItem",
        map_id="map_bling_nota_fiscal_item",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Parcela",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalParcela",
        map_id="map_bling_nota_fiscal_parcela",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),

    Job(
        name="Carga STG Bling Produtos",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Produtos",
        map_id="map_bling_produtos",
        endpoint="produtos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
    ),













]

PROCS: List[List[Dict[str, Any]]] = [
    
    # GRUPO 1: Carga das Tabelas de Dimensão.
    [
        # {"db_name": "DB_DW_NAME", "sql": "CALL spCarga_DimDREFormatos"},
    ],
    
    # GRUPO 2: Carga das Tabelas Fato.
    # [
    #     {"db_name": "HASHTAG_DW", "sql": "CALL spCarga_FatoOrcado"},
    # ]
]