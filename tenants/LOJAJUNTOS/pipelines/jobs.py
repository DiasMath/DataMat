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
    param_matrix: Optional[Dict[str, List[Any]]] = None
    incremental_config: Optional[Dict[str, Any]] = None

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
    max_passes: int = 1
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
        full_load_weekday=6,
    ),

    # ========= CONTAS ========= #
    Job(
        name="Carga STG Bling Contas a Pagar", # depois tirar porque já está em 2026
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
        full_load_weekday=6,
        params={
            "dataVencimentoInicial": "2025-01-01",
            "dataVencimentoFinal": "2025-12-31",
        },
        param_matrix={ "situacao": [1, 2, 3, 4, 5] },
        incremental_config={
            "enabled": False,
            "date_param_start": "dataVencimentoInicial",
            "date_param_end": "dataVencimentoFinal",
            "days_to_load": 90
        }
    ),  

    Job(
        name="Carga STG Bling Contas a Pagar 2026+",
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
        # full_load_weekday=6,
        params={
            "dataVencimentoInicial": "2026-01-01",  # Já pega tudo pra frente 
        },
        param_matrix={ "situacao": [1, 2, 3, 4, 5] },
        incremental_config={
            "enabled": False,
            "date_param_start": "dataVencimentoInicial",
            "date_param_end": "dataVencimentoFinal",
            "days_to_load": 90
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
        full_load_weekday=6, 
        max_passes=1,
        params={
            "tipoFiltroData": "V",
            "dataInicial": "2025-01-01",
            "dataFinal": "2025-12-31",
        },
        param_matrix={
            "situacoes[]": [1, 2, 3, 4, 5],
        },
        incremental_config={
            "enabled": False,
            "date_param_start": "dataInicial",
            "date_param_end": "dataFinal",
            "days_to_load": 90
        }
    ),

    Job(
        name="Carga STG Bling Contas a Receber 2026",
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
        max_passes=1,
        params={
            "tipoFiltroData": "V",
            "dataInicial": "2026-01-01",
            "dataFinal": "2026-12-31",
        },
        param_matrix={
            "situacoes[]": [1, 2, 3, 4, 5],
        },
        incremental_config={
            "enabled": False,
            "date_param_start": "dataInicial",
            "date_param_end": "dataFinal",
            "days_to_load": 90
        }
    ),

    Job(
        name="Carga Temporaria STG Bling File Contas Pagar e Receber",
        map_id="temp_map_bling_drive_contas_pagar_e_receber",
        type="file",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_DRIVE_MovimentacoesFinanceiras",
        file="tenants/LOJAJUNTOS/data/movimentacoes_financeiras.csv",
        delimiter=';',
        truncate=True
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
        full_load_weekday=6
    ),


    # ========= FORMAS PAGAMENTO ========= #
    # Job(
    #     name="Carga STG Bling Formas Pagamento",
    #     map_id="map_bling_formas_pagamento",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_FormasPagamento",
    #     endpoint="formas-pagamentos",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    # ),

    # # ========= NOTA FISCAL ========= #
    # Job(
    #     name="Carga STG Bling Nota Fiscal",
    #     map_id="map_bling_nota_fiscal",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscal",
    #     endpoint="nfe",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params={
    #         # "dataEmissaoInicial": "2020-01-01",
    #         # "dataEmissaoFinal": "2020-12-31",
    #     },
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    #         "tipo": [0, 1]
    #     },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Nota Fiscal Item",
    #     map_id="map_bling_nota_fiscal_item",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscalItem",
    #     endpoint="nfe",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params = {
    #         # "dataEmissaoInicial" : "2020-01-01",
    #         # "dataEmissaoFinal" : "2020-12-31"
    #     },
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    #         "tipo": [0, 1]
    #     },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Nota Fiscal Parcela",
    #     map_id="map_bling_nota_fiscal_parcela",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscalParcela",
    #     endpoint="nfe",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params = {
    #         # "dataEmissaoInicial" : "2020-01-01",
    #         # "dataEmissaoFinal" : "2020-12-31"
    #     },
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    #         "tipo": [0, 1]
    #     },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # ========= NOTA FISCAL CONSUMIDOR ========= #
    # Job(
    #     name="Carga STG Bling Nota Fiscal Consumidor",
    #     map_id="map_bling_nota_fiscal_consumidor",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscalConsumidor",
    #     endpoint="nfce",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    #         "tipo": [0, 1]
    #     },
    #     # params = {
    #     # "dataEmissaoInicial" : "2026-01-01",
    #     # "dataEmissaoFinal" : "2026-12-31"
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Nota Fiscal Consumidor Item",
    #     map_id="map_bling_nota_fiscal_consumidor_item",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscalConsumidorItem",
    #     endpoint="nfce",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12],
    #         "tipo": [0, 1]
    #     },
    #     # params = {
    #     # "dataEmissaoInicial" : "2025-01-01",
    #     # "dataEmissaoFinal" : "2025-12-31"
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Nota Fiscal Consumidor Parcela",
    #     map_id="map_bling_nota_fiscal_consumidor_parcela",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NotaFiscalConsumidorParcela",
    #     endpoint="nfce",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     param_matrix={
    #         "situacao": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12],
    #         "tipo": [0, 1]
    #     },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataEmissaoInicial",
    #         "date_param_end": "dataEmissaoFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # ========= PRODUTOS ========= #
    # Job(
    #     name="Carga STG Bling Produtos",
    #     map_id="map_bling_produtos",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_Produtos",
    #     endpoint="produtos",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params = { 
    #         "criterio" : 5, 
    #         # "tipo" : "T" , 
    #         # "dataInclusaoInicial" : "2025-01-01", 
    #         # "dataInclusaoFinal" : "2025-12-31"
    #         }
    # ),

    # Job(
    #     name="Carga STG Bling Produtos Grupos",
    #     map_id="map_bling_produto_grupo",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_ProdutoGrupo",
    #     endpoint="grupos-produtos",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    # ),

    # Job(
    #     name="Carga STG Bling Produtos Categorias",
    #     map_id="map_bling_produto_categoria",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_ProdutoCategoria",
    #     endpoint="categorias/produtos",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    # ),

    # Job(
    #     name="Carga STG Bling Produtos Fornecedor",
    #     map_id="map_bling_produto_fornecedor",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_ProdutoFornecedor",
    #     endpoint="produtos/fornecedores",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    # ),

    # Job(
    #     name="Carga STG Bling Produtos Tributacao",
    #     map_id="map_bling_produto_tributacao",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_ProdutoTributacao",
    #     endpoint="produtos",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    # ),

    # ========= CONTATO ========= #
    Job(
        name="Carga STG Bling Contatos",
        map_id="map_bling_contato",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Contatos",
        endpoint="contatos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path= "data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150,
        params={ "criterio" : 1}
    ),

    Job(
        name="Carga STG Bling Contato Tipos Contato",
        map_id="map_bling_contato_tipos_contato",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_ContatoTiposContato",
        endpoint="contatos",
        auth=BLING_OAUTH_CONFIG,
        paging=PAGING,
        enrich_by_id=True,
        enrichment_strategy='sequential',
        data_path="data",
        detail_data_path="data",
        requests_per_minute=180,
        enrichment_requests_per_minute=150
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
        data_path= "data",
        requests_per_minute=180,
    ),

    Job(
        name="Carga STG Bling Contato Consumidor Final",
        map_id="map_bling_contato_consumidor_final",
        type="api",
        db_name="DB_STG_NAME",
        table="tbSTG_BLING_Contatos",
        endpoint="contatos/consumidor-final",
        auth=BLING_OAUTH_CONFIG,
        data_path= "data",
        requests_per_minute=180,
        params={ "criterio" : 1}
    ),

    # ========= NATUREZA OPERACAO ========= #
    # Job(
    #     name="Carga STG Bling Natureza Operacao",
    #     map_id="map_bling_natureza_operacao",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_NaturezaOperacao",
    #     endpoint="naturezas-operacoes",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     data_path= "data",
    #     requests_per_minute=180,
    # ),

    # ========= VENDEDORES ========= #
    # Job(
    #     name="Carga STG Bling Vendedores",
    #     map_id="map_bling_vendedores",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_Vendedores",
    #     endpoint="vendedores",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params = { "situacaoContato" : "T"}
    # ),

   # ========= PEDIDOS ========= #
    # Job(
    #     name="Carga STG Bling Pedido Compra",
    #     map_id="map_bling_pedido_compra",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_PedidoCompra",
    #     endpoint="pedidos/compras",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     # params = {
    #     #     "dataInicial" : "2025-01-01",
    #     #     "dataFinal" : "2025-12-31"
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataInicial",
    #         "date_param_end": "dataFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Pedido Compra Item",
    #     map_id="map_bling_pedido_compra_item",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_PedidoCompraItem",
    #     endpoint="pedidos/compras",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     # params = {
    #     # "dataInicial" : "2025-01-01",
    #     # "dataFinal" : "2025-12-31"
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataInicial",
    #         "date_param_end": "dataFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Pedido Venda",
    #     map_id="map_bling_pedido_venda",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_PedidoVenda",
    #     endpoint="pedidos/vendas",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     # params = {
    #     # "dataInicial" : "2025-01-01",
    #     # "dataFinal" : "2025-12-31"
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataInicial",
    #         "date_param_end": "dataFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Pedido Venda Item",
    #     map_id="map_bling_pedido_venda_item",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_PedidoVendaItem",
    #     endpoint="pedidos/vendas",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     # params={
    #     #     "dataInicial": "2025-01-01",
    #     #     "dataFinal": "2025-12-31",
    #     # },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataInicial",
    #         "date_param_end": "dataFinal",
    #         "days_to_load": 30
    #     }
    # ),

    # Job(
    #     name="Carga STG Bling Pedido Venda Parcela",
    #     map_id="map_bling_pedido_venda_parcela",
    #     type="api",
    #     db_name="DB_STG_NAME",
    #     table="tbSTG_BLING_PedidoVendaParcela",
    #     endpoint="pedidos/vendas",
    #     auth=BLING_OAUTH_CONFIG,
    #     paging=PAGING,
    #     enrich_by_id=True,
    #     enrichment_strategy='sequential',
    #     data_path= "data",
    #     detail_data_path="data",
    #     requests_per_minute=180,
    #     enrichment_requests_per_minute=150,
    #     params={
    #         # "dataInicial": "2025-01-01",
    #         # "dataFinal": "2025-12-31",
    #     },
    #     incremental_config={
    #         "enabled": True,
    #         "date_param_start": "dataInicial",
    #         "date_param_end": "dataFinal",
    #         "days_to_load": 30
    #     }
    # ),
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
        #{"db_name": "DB_DW_NAME", "file": "procedures/spCarga_DimContaFinanceira.sql"},
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
# PROCS = [
#       # GRUPO 1: Carga das Tabelas de Dimensão.
#     # [
#     #     {
#     #         "name": "spCarga_DimDREFormatos", # Nome da procedure no banco
#     #         # Configuração incremental opcional
#     #         "incremental_config": {
#     #             "enabled": True,
#     #             "date_column": "DataReferencia", # Coluna de data na tabela STG
#     #             "days_to_load": 30
#     #         }
#     #     },
#     #     # Exemplo de uma procedure que sempre fará carga completa
#         # {"name": "spSetup_DimTempo('2015-01-01', '2035-12-31')"},
#         # {"name": "spSetup_DimCfop()"},
#         {"name": "spCarga_MainDW", "sql": "spCarga_MainDW"}
    
#     # ]
# ]

PROC_GRP_GERAL = [
    {"name": "spCarga_MainDW", "sql": "spCarga_MainDW", "use_out_params": False},
]

PROCS = [
    PROC_GRP_GERAL
]