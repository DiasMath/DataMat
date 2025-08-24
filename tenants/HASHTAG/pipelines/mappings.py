from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass(frozen=True)
class MappingSpec:
    # nomes das colunas na ORIGEM (arquivo/API/DB) -> nomes na TABELA ALVO (destino)
    src_to_tgt: Dict[str, str]
    # nomes já no padrão do DESTINO
    required: Optional[List[str]] = None
    key_cols: Optional[List[str]] = None               # devem existir como PK/UNIQUE no alvo
    compare_cols: Optional[List[str]] = None           # se None: todas as não-chave

# ------------------------------------------------------------
#  CATÁLOGO DE MAPEAMENTOS
#  Use a chave como map_id nos jobs.
# ------------------------------------------------------------
MAPPINGS: Dict[str, MappingSpec] = {

# ===================== CARGA STAGE ===================== #

    # ============= FINANCEIRO ============= #
    "map_api_financeiro_clientes": MappingSpec(
        src_to_tgt={
            "id"           : "IdStg",
            "razaoSocial"  : "RazaoSocial",
            "nomeFantasia" : "NomeFantasia",
            "tipoPessoa"   : "TipoPessoa",
            "municipio"    : "Municipio",
            "UF"           : "UF",
        },
        required=["RazaoSocial"],
        key_cols=["RazaoSocial"],           
        compare_cols=None,                  
    ),
    "map_api_financeiro_fornecedores": MappingSpec(
        src_to_tgt={
            "id"           : "IdStg",
            "razaoSocial"  : "RazaoSocial",
            "nomeFantasia" : "NomeFantasia",
            "tipoPessoa"   : "TipoPessoa",
            "municipio"    : "Municipio",
            "UF"           : "UF",
        },
        required=["RazaoSocial"],
        key_cols=["RazaoSocial"],           
        compare_cols=None,                
    ),
    "map_api_financeiro_bancos": MappingSpec(
        src_to_tgt={
            "id"            : "IdStg",
            "contaBancaria" : "ContaBancaria",
            "nomeBanco"     : "NomeBanco",
            "municipio"     : "Municipio",
            "UF"            : "UF",
        },
        required=["ContaBancaria"],
        key_cols=["ContaBancaria"],           
        compare_cols=None,                
    ),
    "map_api_financeiro_pagamentos": MappingSpec(
        src_to_tgt={
            "id"                : "IdStg",
            "contaBancaria"     : "ContaBancaria",
            "idFornecedor"      : "IdFinanceiroFornecedor",
            "dataEmissao"       : "DataEmissao",
            "dataVencimento"    : "DataVencimento",
            "dataMovimentacao"  : "DataMovimentacao",
            "valorMovimentacao" : "ValorMovimentacao",
        },
        required=["ContaBancaria"],
        key_cols=["IdFinanceiroFornecedor", "ContaBancaria", "DataMovimentacao", "ValorMovimentacao"],           
        compare_cols=None,                
    ),
    "map_api_financeiro_recebimentos": MappingSpec(
        src_to_tgt={
            "id"                : "IdStg",
            "idCliente"         : "IdFinanceiroCliente",
            "contaBancaria"     : "ContaBancaria",
            "dataEmissao"       : "DataEmissao",
            "dataVencimento"    : "DataVencimento",
            "dataMovimentacao"  : "DataMovimentacao",
            "valorMovimentacao" : "ValorMovimentacao",
        },
        required=["ContaBancaria"],
        key_cols=["IdFinanceiroCliente", "ContaBancaria", "DataMovimentacao", "ValorMovimentacao"],           
        compare_cols=None,                
    ),

    # ============= DRE ============= #
    "map_api_dre_formatos": MappingSpec(
        src_to_tgt={
            "id"        : "IdStg",
            "ordem"     : "Ordem",
            "grupo"     : "Grupo",
            "subtotal"  : "Subtotal",
        },
        required=["Grupo"],
        key_cols=["Grupo"],           
        compare_cols=None,                  
    ),
    "map_api_dre_plano_contas": MappingSpec(
        src_to_tgt={
            "id"                : "IdStg",
            "conta"             : "Conta",
            "descricaoConta"   : "DescricaoConta",
            "nivel1"            : "Nivel1",
            "nivel2"            : "Nivel2",
        },
        required=["Conta"],
        key_cols=["Conta"],           
        compare_cols=None,                  
    ),
    "map_api_dre_orcados": MappingSpec(
        src_to_tgt={
            "id"            : "IdStg",
            "dataOrcado"    : "DataOrcado",
            "conta"         : "Conta",
            "valorOrcado"   : "ValorOrcado",
        },
        required=["Conta"],
        key_cols=["DataOrcado", "Conta", "ValorOrcado"],           
        compare_cols=None,                  
    ),
    "map_api_dre_realizados": MappingSpec(
        src_to_tgt={
            "id"                : "IdStg",
            "dataRealizado"     : "DataRealizado",
            "conta"             : "Conta",
            "valorRealizado"    : "ValorRealizado",
        },
        required=["Conta"],
        key_cols=["DataRealizado", "Conta", "ValorRealizado"],           
        compare_cols=None,                  
    ),

}
