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
    params: Optional[Dict[str, Any]] = None

    # ---- origem (API) ----
    endpoint: Optional[str] = None
    auth: Optional[Dict[str, Any]] = None # Configuração de autenticação
    paging: Optional[Dict[str, Any]] = None   # Manter só se a API tiver paginação REAL
    timeout: Optional[int] = None

    # ---- incremental (push-down e fallback) ----
    date_field: Optional[str] = None
    from_date: Optional[str] = None

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
PAGING: Dict[str, Any] = {
    "mode": "page",
    "page_param": "page",
    "size_param": "limit",
    "size": 100,
}

# ====================================================================
#  LISTA DE JOBS DE EXTRAÇÃO
# ====================================================================
JOBS: List[Job] = [

# ===================== CARGA STAGE ===================== #

    # ============= FINANCEIRO ============= #
    Job(
        name="Carga STG Financeiro Clientes",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_FinanceiroClientes",
        map_id="map_api_financeiro_clientes",       
        endpoint="financeiro-clientes",
        auth=None,                             
        paging=PAGING,
    ),
    Job(
        name="Carga STG Financeiro Fornecedores",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_FinanceiroFornecedores",
        map_id="map_api_financeiro_fornecedores",       
        endpoint="financeiro-fornecedores",
        auth=None,                             
        paging=PAGING,
    ),
    Job(
        name="Carga STG Financeiro Bancos",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_FinanceiroBancos",
        map_id="map_api_financeiro_bancos",       
        endpoint="financeiro-bancos",
        auth=None,                             
        paging=PAGING,
    ),
    Job(
        name="Carga STG Financeiro Recebimentos",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_FinanceiroRecebimentos",
        map_id="map_api_financeiro_recebimentos",       
        endpoint="financeiro-recebimentos",
        auth=None,                             
        paging=PAGING,
    ),
    Job(
        name="Carga STG Financeiro Pagamentos",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_FinanceiroPagamentos",
        map_id="map_api_financeiro_pagamentos",       
        endpoint="financeiro-pagamentos",
        auth=None,                             
        paging=PAGING,
    ),

    # ============= DRE ============= #
    Job(
        name="Carga STG DRE Formatos",
        type="api",
        db_name="HASHTAG_STG",                      
        table="tbSTG_DREFormatos",
        map_id="map_api_dre_formatos",       
        endpoint="dre-formatos",
        auth=None,                             
        paging=PAGING,
    ),
    Job(
        name="Carga STG DRE Plano Contas",
        type="api",
        db_name="HASHTAG_STG",           
        table="tbSTG_DREPlanoContas",
        map_id="map_api_dre_plano_contas",       
        endpoint="dre-plano-contas",
        auth=None,
        paging=PAGING,
    ),
    Job(
        name="Carga STG DRE Orcados",
        type="api",
        db_name="HASHTAG_STG",           
        table="tbSTG_DREOrcados",
        map_id="map_api_dre_orcados",      
        endpoint="dre-orcados",
        auth=None,                  
        paging=PAGING,
    ),
    Job(
        name="Carga STG DRE Realizados",
        type="api",
        db_name="HASHTAG_STG",             
        table="tbSTG_DRERealizados",
        map_id="map_api_dre_realizados",      
        endpoint="dre-realizados",
        auth=None,
        paging=PAGING,
    ),

]

# Procedures (string SQL ou dict). Exemplo:
PROCS: List[Any] = [
    # {"db_name": "DB_DW_NAME", "sql": "CALL dw.sp_pos_carga_clientes();"},
]
