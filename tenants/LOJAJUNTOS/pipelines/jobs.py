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

    # ---- origem (API) ----
    endpoint: Optional[str] = None
    auth_env: Optional[str] = None
    paging: Optional[Dict[str, Any]] = None   # manter só se a API tiver paginação REAL
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
    params: Optional[Dict[str, Any]] = None


JOBS: List[Job] = [
    Job(
        name="Carga STG Financeiro Clientes",
        type="api",
        db_name="DB_STG_NAME",                      
        table="tbSTG_FinanceiroClientes",
        map_id="map_api_financeiro_clientes",       
        endpoint="${API_BASE_URL}/financeiro-clientes",
        auth_env=None,                             
        timeout=None,                          
        paging={
                "mode": "page",
                "page_param": "page",
                "size_param": "page_size",
                "size": 200,
                # "base_params": {"status": "ativo"}  # se precisar
                },
        # date_field="updated_at",                    # campo de data da FONTE (API)
        # from_date="",


    #         paging={
    #     "mode": "cursor",
    #     "cursor_path": "meta.next",   # onde está o cursor no JSON
    #     "cursor_param": "cursor",     # param enviado na próxima chamada
    #     "base_params": {}
    # }


    ),
]

# Procedures opcionais (string SQL ou dict). Exemplo:
PROCS: List[Any] = [
    # {"db_name": "DB_DW_NAME", "sql": "CALL dw.sp_pos_carga_clientes();"},
]






    # #===================== COMPRAS =====================#
    # Job(
    #     name="Compra Compradores",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Compras/Base Compras.xlsx",
    #     sheet="dComprador",
    #     table="compra_compradores",
    #     map_id="Compra Compradores",
    # ),
    # Job(
    #     name="Compra Fornecedores",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Compras/Base Compras.xlsx",
    #     sheet="dFornecedor",
    #     table="compra_fornecedores",
    #     map_id="Compra Fornecedores",
    # ),
    # Job(
    #     name="Compra Materia Primas",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Compras/Base Compras.xlsx",
    #     sheet="dMateriaPrima",
    #     table="compra_materia_primas",
    #     map_id="Compra Materia Primas",
    # ),
    # Job(
    #     name="Compras",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Compras/Base Compras.xlsx",
    #     sheet="fCompras",
    #     table="compras",
    #     map_id="Compras",
    # ),

    #===================== DRE =====================#
    # Job(
    #     name="DRE Plano Contas",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/DRE/DRE.xlsx",
    #     sheet="Plano de Contas",
    #     table="dre_plano_contas",
    #     map_id="DRE Plano Contas",
    # ),
    # Job(
    #     name="DRE Formatos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/DRE/DRE.xlsx",
    #     sheet="Formato DRE",
    #     table="dre_formatos",
    #     map_id="DRE Formatos",
    # ),
    # Job(
    #     name="DRE Realizados",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/DRE/DRE.xlsx",
    #     sheet="Realizado",
    #     table="dre_realizados",
    #     map_id="DRE Realizados",
    # ),
    # Job(
    #     name="DRE Orcados",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/DRE/DRE.xlsx",
    #     sheet="Orçado",
    #     table="dre_orcados",
    #     map_id="DRE Orcados",
    # ),

    # #===================== ESTOQUE =====================#
    # Job(
    #     name="Estoque Movimentos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Estoque/BaseDados.xlsx",
    #     sheet="fEstoque",
    #     table="estoque_movimentos",
    #     map_id="Estoque Movimentos",
    # ),
    # Job(
    #     name="Estoque Lojas",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Estoque/BaseDados.xlsx",
    #     sheet="dLoja",
    #     table="estoque_lojas",
    #     map_id="Estoque Lojas",
    # ),
    # Job(
    #     name="Estoque Produtos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Estoque/BaseDados.xlsx",
    #     sheet="dProduto",
    #     table="estoque_produtos",
    #     map_id="Estoque Produtos",
    # ),
    # Job(
    #     name="Estoque Minimo Produtos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Estoque/EstoqueMin.xlsx",
    #     sheet="Estoque Minimo",
    #     table="estoque_minimo_produtos",
    #     map_id="Estoque Minimo Produtos",
    # ),

    #===================== FINANCEIRO =====================#
    # Job(
    #     name="Financeiro Clientes",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Financeiro/Base Financeiro.xlsx",
    #     sheet="Cliente",
    #     table="financeiro_clientes",
    #     map_id="Financeiro Clientes",
    # ),
    # Job(
    #     name="Financeiro Bancos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Financeiro/Base Financeiro.xlsx",
    #     sheet="Banco",
    #     table="financeiro_bancos",
    #     map_id="Financeiro Bancos",
    # ),
    # Job(
    #     name="Financeiro Fornecedores",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Financeiro/Base Financeiro.xlsx",
    #     sheet="Fornecedor",
    #     table="financeiro_fornecedores",
    #     map_id="Financeiro Fornecedores",
    # ),
    # Job(
    #     name="Financeiro Pagamentos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Financeiro/Base Financeiro.xlsx",
    #     sheet="Pagamentos",
    #     table="financeiro_pagamentos",
    #     map_id="Financeiro Pagamentos",
    # ),
    # Job(
    #     name="Financeiro Recebimentos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Financeiro/Base Financeiro.xlsx",
    #     sheet="Recebimentos",
    #     table="financeiro_recebimentos",
    #     map_id="Financeiro Recebimentos",
    # ),

    # #===================== LOGISTICA =====================#
    # Job(
    #     name="Logistica Clientes",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Logística/Base Logistica.xlsx",
    #     sheet="dCliente",
    #     table="logistica_clientes",
    #     map_id="Logistica Clientes",
    # ),
    # Job(
    #     name="Logistica Veículos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Logística/Base Logistica.xlsx",
    #     sheet="dVeiculo",
    #     table="logistica_veiculos",
    #     map_id="Logistica Veiculos",
    # ),
    # Job(
    #     name="Logistica Fretes",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Logística/Base Logistica.xlsx",
    #     sheet="fFrete",
    #     table="logistica_fretes",
    #     map_id="Logistica Fretes",
    # ),
    # Job(
    #     name="Logistica Movimentos",
    #     type="file",
    #     file="tenants/HASHTAG/data/PLANILHAS/Logística/Base Logistica.xlsx",
    #     sheet="fKmRodado",
    #     table="logistica_movimentos",
    #     map_id="Logistica Movimentos",
    # ),







    

# Procedures SQL a executar APÓS os loads (ordem importa) — opcional
# PROCS: List[str] = [
    # "CALL prc_hashtag_upsert_dim_clientes();",
    # "CALL prc_hashtag_upsert_fato_recebimentos();",
# ]