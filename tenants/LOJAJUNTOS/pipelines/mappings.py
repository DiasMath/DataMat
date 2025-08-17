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
        key_cols=["RazaoSocial"],           # chave de merge (garanta UNIQUE/PK na tabela alvo)
        compare_cols=None,                  # None => compara todas as não-chave no merge
    ),






    # # ===================== COMPRAS ===================== #
    # "Compra Compradores": MappingSpec(
    #     src_to_tgt={
    #         "Codigo Comprador": "codigo_comprador",
    #         "Nome Comprador":   "nome_comprador",
    #     },
    #     required=["codigo_comprador", "nome_comprador"],
    #     key_cols=["codigo_comprador"],
    #     compare_cols= None,
    # ),
    # "Compra Fornecedores": MappingSpec(
    #     src_to_tgt={
    #         "Codigo Fornecedor": "codigo_fornecedor",
    #         "Nome Fornecedor":   "nome_fornecedor",
    #     },
    #     required=["codigo_fornecedor", "nome_fornecedor"],
    #     key_cols=["codigo_fornecedor"],
    #     compare_cols= None,
    # ),
    # "Compra Materia Primas": MappingSpec(
    #     src_to_tgt={
    #         "Codigo MP":     "codigo_materia_prima",
    #         "Nome MP":       "nome_materia_prima",
    #         "Custo Unitário":"custo_unitario",
    #     },
    #     required=["codigo_materia_prima", "nome_materia_prima", "custo_unitario"],
    #     key_cols=["codigo_materia_prima"],
    #     compare_cols= None,
    # ),
    # "Compras": MappingSpec(
    #     src_to_tgt={
    #         "Pedido":             "codigo_pedido",
    #         "Data Pedido":        "data_pedido",
    #         "Data Saída":         "data_saida",
    #         "Previsão Entrega":   "data_previsao_entrega",
    #         "Data Entrega":       "data_entrega",
    #         "Comprador":          "compra_comprador_id",
    #         "Materia Prima":      "compra_materia_prima_id",
    #         "Quantidade":         "quantidade",
    #         "Desconto (%)":       "desconto",
    #         "Fornecedor":         "compra_fornecedor_id",
    #     },
    #     required=["codigo_pedido"],
    #     key_cols=["codigo_pedido"],
    #     compare_cols= None,
    # ),

    # ======================= DRE ======================= #
    # "DRE Plano Contas": MappingSpec(
    #     src_to_tgt={
    #         "Conta":               "conta",
    #         "Descrição da Conta":  "descricao_conta",
    #         "Nível 1":             "nivel1",
    #         "Nível 2":             "nivel2",
    #     },
    #     required=["conta"],
    #     key_cols=["conta"],
    #     compare_cols= None,
    # ),
    # "DRE Formatos": MappingSpec(
    #     src_to_tgt={
    #         "Ordem":    "ordem",
    #         "Grupo":    "grupo",
    #         "Subtotal": "subtotal",
    #     },
    #     required=["grupo"],
    #     key_cols=["grupo"],
    #     compare_cols= None,
    # ),
    # "DRE Realizados": MappingSpec(
    #     src_to_tgt={
    #         "ID":              "id",
    #         "Mês/Ano":         "data_realizado",
    #         "Conta":           "conta",
    #         "Valor Realizado": "valor_realizado",
    #     },
    #     required=["conta"],
    #     key_cols=["id"],
    #     compare_cols= None,
    # ),
    # "DRE Orcados": MappingSpec(
    #     src_to_tgt={
    #         "ID":            "id",
    #         "Mês/Ano":       "data_orcado",
    #         "Conta":         "conta",
    #         "Valor Orçado":  "valor_orcado",
    #     },
    #     required=["conta"],
    #     key_cols=["id"],
    #     compare_cols= None,
    # ),

    # # ===================== ESTOQUE ===================== #
    # "Estoque Movimentos": MappingSpec(
    #     src_to_tgt={
    #         "ID Movimentação": "id",
    #         "ID Produto":      "estoque_produto_id",
    #         "Data":            "data_movimento",
    #         "ID Loja":         "estoque_loja_id",
    #         "Movimentação":    "movimentacao",
    #         "Tipo":            "tipo_movimento",
    #     },
    #     required=["estoque_produto_id"],
    #     key_cols=["id"],
    #     compare_cols= None,
    # ),
    # "Estoque Lojas": MappingSpec(
    #     src_to_tgt={
    #         "ID Loja": "id",
    #         "Loja":    "loja",
    #         "Bairro":  "bairro",
    #     },
    #     required=["loja"],
    #     key_cols=["loja"],
    #     compare_cols= None,
    # ),
    # "Estoque Produtos": MappingSpec(
    #     src_to_tgt={
    #         "ID Produto":  "id",
    #         "Produto":     "produto",
    #         "Categoria":   "categoria",
    #         "Subcategoria":"subcategoria",
    #         "Custo Unit":  "custo_unitario",
    #         "Preço Unit":  "preco_unitario",
    #     },
    #     required=["produto"],
    #     key_cols=["produto"],
    #     compare_cols= None,
    # ),
    # "Estoque Minimo Produtos": MappingSpec(
    #     src_to_tgt={
    #         "ID Produto":     "estoque_produto_id",
    #         "Estoque Mínimo": "estoque_minimo",
    #     },
    #     required=["estoque_produto_id"],
    #     key_cols=["estoque_produto_id"],
    #     compare_cols= None,
    # ),

    # ==================== FINANCEIRO =================== #
    # "Financeiro Clientes": MappingSpec(
    #     src_to_tgt={
    #         "Id Cliente":   "id",
    #         "Razao Social": "razao_social",
    #         "Nome Fantasia":"nome_fantasia",
    #         "Tipo Pessoa":  "tipo_pessoa",
    #         "Municipio":    "municipio",
    #         "UF":           "UF",
    #     },
    #     required=["razao_social"],
    #     key_cols=["razao_social"],
    #     compare_cols= None,
    # ),
    # "Financeiro Bancos": MappingSpec(
    #     src_to_tgt={
    #         "Id Banco":           "id",
    #         "Id Conta Bancária":  "conta_bancaria",
    #         "Nome Banco":         "nome_banco",
    #         "Município":          "municipio",
    #         "UF":                 "UF",
    #     },
    #     required=["conta_bancaria"],
    #     key_cols=["conta_bancaria"],
    #     compare_cols= None,
    # ),
    # "Financeiro Fornecedores": MappingSpec(
    #     src_to_tgt={
    #         "Id Fornecedor":    "id",
    #         "Razao Social":     "razao_social",
    #         "Nome Fantasia":    "nome_fantasia",
    #         "Tipo Pessoa":      "tipo_pessoa",
    #         "Municipio":        "municipio",
    #         "UF":               "UF",
    #     },
    #     required=["razao_social"],
    #     key_cols=["razao_social"],
    #     compare_cols= None,
    # ),
    # "Financeiro Pagamentos": MappingSpec(
    #     src_to_tgt={
    #         "Id Fornecedor":            "financeiro_fornecedor_id",
    #         "Id Conta Bancária":        "conta_bancaria",
    #         "Data de Emissao":          "data_emissao",
    #         "Data de Vencimento":       "data_vencimento",
    #         "Data da Movimentação":     "data_movimentacao",
    #         "Valor da Movimentação":    "valor_movimentacao",
    #     },
    #     required=["conta_bancaria"],
    #     key_cols=["financeiro_fornecedor_id", "conta_bancaria", "data_movimentacao", "valor_movimentacao"],
    #     compare_cols= None,
    # ),
    # "Financeiro Recebimentos": MappingSpec(
    #     src_to_tgt={
    #         "Id Cliente":               "financeiro_cliente_id",
    #         "Id Conta Bancária":        "conta_bancaria",
    #         "Data de Emissao":          "data_emissao",
    #         "Data de Vencimento":       "data_vencimento",
    #         "Data da Movimentação":     "data_movimentacao",
    #         "Valor da Movimentação":    "valor_movimentacao",
    #     },
    #     required=["conta_bancaria"],
    #     key_cols=["financeiro_cliente_id", "conta_bancaria", "data_movimentacao", "valor_movimentacao"],
    #     compare_cols= None,
    # ),









    # # ==================== LOGISTICA =================== #
    # "Logistica Clientes": MappingSpec(
    #     src_to_tgt={
    #         "ID Cliente":   "id",
    #         "Cidade":       "cidade",
    #         "UF":           "UF",
    #     },
    #     required=["id"],
    #     key_cols=["cidade"],
    #     compare_cols= None,
    # ),
    # "Logistica Veiculos": MappingSpec(
    #     src_to_tgt={
    #         "ID Veiculo":       "id",
    #         "Placa":            "placa",
    #         "Marca":            "marca",
    #         "Tipo Veículo":     "tipo_veiculo",
    #         "Baú":              "bau",
    #     },
    #     required=["placa"],
    #     key_cols=["placa"],
    #     compare_cols= None,
    # ),
    # "Logistica Fretes": MappingSpec(
    #     src_to_tgt={
    #         "Data":                     "data_viagem",
    #         "ID Cliente":               "logistica_cliente_id",
    #         "ID Veiculo":               "logistica_veiculo_id",
    #         "Numero Documento Fiscal":  "numero_documento_fiscal",
    #         "Viagem":                   "codigo_viagem",
    #         "Valor do Frete Líquido":   "valor_frete_liquido",
    #         "Peso (KG)":                "peso",
    #         "Valor da Mercadoria":      "valor_mercadoria",
    #     },
    #     required=["codigo_viagem"],
    #     key_cols=["codigo_viagem"],
    #     compare_cols= None,
    # ),
    # "Logistica Movimentos": MappingSpec(
    #     src_to_tgt={
    #         "Mês":                  "data_movimento",
    #         "ID Veiculo":           "logistica_veiculo_id",
    #         "Km percorridos":       "km_percorrido",
    #         "Gasto com Combustível": "gasto_combustivel",
    #         "Manut.":               "manutencao",
    #         "Custos Fixos":         "custos_fixos",
    #     },
    #     required=["logistica_veiculo_id"],
    #     key_cols=["data_movimento", "logistica_veiculo_id", "km_percorrido"],
    #     compare_cols= None,
    # ),




    
}
