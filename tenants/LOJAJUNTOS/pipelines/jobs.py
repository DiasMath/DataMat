from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import os

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

    # --- Parâmetros Específicos por Tipo de Adapter ---
    # API
    endpoint: Optional[str] = None
    auth: Optional[Dict[str, Any]] = None
    paging: Optional[Dict[str, Any]] = None
    data_path: Optional[str] = None
    detail_data_path: Optional[str] = None
    enrich_by_id: bool = False
    enrichment_strategy: str = 'concurrent'  # Estratégia: 'concurrent' ou 'sequential'
    requests_per_minute: Optional[int] = 60  # Limite de requisições para a extração principal
    enrichment_requests_per_minute: Optional[int] = None # Limite para o enriquecimento (se None, usa o principal)
    delay_between_pages_ms: Optional[int] = None

    # Arquivo (File)
    file: Optional[str] = None
    sheet: Optional[str] = None

    # Banco de Dados (DB)
    source_url: Optional[str] = None
    query: Optional[str] = None


# ===================================================================
# DEFINIÇÃO DOS JOBS DE EXTRAÇÃO (CARGA PARA STAGING AREA)
# ===================================================================
# Esta lista define todas as fontes de dados que serão extraídas.
# O orquestrador irá iterar por esta lista e executar cada job,
# idealmente em paralelo.

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

    # ========= CATEGORIAS FINANCEIRAS ========= #
    Job(
        name="Carga STG Bling Categorias Financeiras",
        map_id="map_bling_categorias_financeiras",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_CategoriasFinanceiras",
        endpoint="categorias/receitas-despesas",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    # ========= CONTAS ========= #
    Job(
        name="Carga STG Bling Contas a Pagar",
        map_id="map_bling_contas_pagar",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasPagar",
        endpoint="contas/pagar",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        delay_between_pages_ms=500, 
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
        params={
            "dataEmissaoInicial": "2026-01-01",
            "dataEmissaoFinal": "2026-12-31",
            # "criterio": 3
        }
    ),  

    Job(
        name="Carga STG Bling Contas a Receber",
        map_id="map_bling_contas_receber",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasReceber",
        endpoint="contas/receber",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
        params={
            # "dataInicial": "2025-01-01",
            # "dataFinal": "2025-12-31",
            # "criterio": 3
        }
    ),

    Job(
        name="Carga STG Bling Contas a Receber Boleto",
        map_id="map_bling_contas_receber_boleto",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasReceberBoleto",
        endpoint="contas/receber/boletos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
        params={
            "dataEmissaoInicial": "2021-01-01",
            "dataEmissaoFinal": "2021-12-31",
            # "criterio": 3
        }
    ),

    Job(
        name="Carga STG Bling Contas Contabeis",
        map_id="map_bling_contas_contabeis",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContasContabeis",
        endpoint="contas-contabeis",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),


    # ========= FORMAS PAGAMENTO ========= #
    Job(
        name="Carga STG Bling Formas Pagamento",
        map_id="map_bling_formas_pagamento",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_FormasPagamento",
        endpoint="formas-pagamentos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    # ========= NOTA FISCAL ========= #
    Job(
        name="Carga STG Bling Nota Fiscal",
        map_id="map_bling_nota_fiscal",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscal",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
        params={
            "dataEmissaoInicial": "2020-01-01",
            "dataEmissaoFinal": "2020-12-31",
            "tipo": 0,
            "criterio": 3
        }
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Item",
        map_id="map_bling_nota_fiscal_item",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalItem",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Parcela",
        map_id="map_bling_nota_fiscal_parcela",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalParcela",
        endpoint="nfe",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    # ========= NOTA FISCAL CONSUMIDOR ========= #
    Job(
        name="Carga STG Bling Nota Fiscal Consumidor",
        map_id="map_bling_nota_fiscal_consumidor",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalConsumidor",
        endpoint="nfce",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Consumidor Item",
        map_id="map_bling_nota_fiscal_consumidor_item",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalConsumidorItem",
        endpoint="nfce",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Nota Fiscal Consumidor Parcela",
        map_id="map_bling_nota_fiscal_consumidor_parcela",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_NotaFiscalConsumidorParcela",
        endpoint="nfce",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    # ========= PRODUTOS ========= #
    Job(
        name="Carga STG Bling Produtos",
        map_id="map_bling_produtos",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Produtos",
        endpoint="produtos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Produtos Grupos",
        map_id="map_bling_produto_grupo",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ProdutoGrupo",
        endpoint="grupos-produtos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Produtos Categorias",
        map_id="map_bling_produto_categoria",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ProdutoCategoria",
        endpoint="categorias/produtos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Produtos Fornecedor",
        map_id="map_bling_produto_fornecedor",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ProdutoFornecedor",
        endpoint="produtos/fornecedores",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Produtos Tributacao",
        map_id="map_bling_produto_tributacao",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ProdutoTributacao",
        endpoint="produtos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    # ========= CONTATO ========= #
    Job(
        name="Carga STG Bling Contatos",
        map_id="map_bling_contato",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Contato",
        endpoint="contatos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Tipo Contato",
        map_id="map_bling_tipo_contato",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContatoTipo",
        endpoint="contatos/tipos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),

    Job(
        name="Carga STG Bling Contato Consumidor Final",
        map_id="map_bling_contato_consumidor_final",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Contato",
        endpoint="contatos/tipos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
    ),




]

# ===================================================================
# DEFINIÇÃO DOS OBJETOS DO DATA WAREHOUSE (Views, Procs, Functions)
# ===================================================================
# Esta lista define a ORDEM DE CRIAÇÃO/ATUALIZAÇÃO dos objetos no banco.
# Grupos são executados em sequência para garantir que as dependências
# sejam resolvidas corretamente. (ex: funções, depois views, depois procs).

DW_OBJECTS = [
    # --- GRUPO 1: Funções e Views base ---
    # Estes objetos não têm dependências ou dependem apenas de tabelas.
    # [
    #     {"db_name": "DB_DW_NAME", "file": "functions/fn_format_date.sql"},
    #     {"db_name": "DB_DW_NAME", "file": "views/vw_base_clientes.sql"},
    # ],
    # # --- GRUPO 2: Views que dependem do Grupo 1 ---
    # [
    #     {"db_name": "DB_DW_NAME", "file": "views/vw_pedidos_enriquecidos.sql"},
    # ],
    # # --- GRUPO 3: Procedures que dependem das Views ---
    # [
    #     {"db_name": "DB_DW_NAME", "file": "procedures/sp_carga_inicial.sql"},
    # ]
]


# ===================================================================
# DEFINIÇÃO DAS PROCEDURES DE CARGA (Ordem de Execução)
# ===================================================================
# Esta lista define a ORDEM de EXECUÇÃO das procedures de carga de dados.
PROCS = [
    # [
    #     {"db_name": "DB_DW_NAME", "sql": "spCarga_DimClientes"},
    #     {"db_name": "DB_DW_NAME", "sql": "spCarga_DimProdutos"},
    # ],
    # [
    #     {"db_name": "DB_DW_NAME", "sql": "spCarga_FatoPedidos"},
    # ]
]