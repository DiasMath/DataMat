from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import pandera.pandas as pa

@dataclass(frozen=True)
class MappingSpec:
    # De-para das colunas da origem para o destino
    src_to_tgt: Dict[str, str]
    # Nomes das colunas de destino que são obrigatórias
    required: Optional[List[str]] = None
    # Colunas que formam a chave para o merge (UPSERT)
    key_cols: Optional[List[str]] = None
    # Colunas a serem comparadas no merge; se None, compara todas as não-chave
    compare_cols: Optional[List[str]] = None
    # Dicionário com as regras de validação do Pandera para cada coluna de DESTINO
    validation_rules: Dict[str, Any] = None


    # {"checks": [pa.Check.isin(["A", "I"])]}

# ==============================================================================
#  CATÁLOGO DE MAPEAMENTOS E VALIDAÇÕES
# ==============================================================================
MAPPINGS: Dict[str, MappingSpec] = {

# ===================== CARGA STAGE ===================== #

    # =========== DRE =========== #
    "map_bling_categorias_financeiras": MappingSpec(
        src_to_tgt={
            "id"                : "CodigoCategoriaFinanceira",
            "idCategoriaPai"    : "IdCategoriaPai",
            "descricao"         : "Descricao",
            "tipo"              : "Tipo",
            "situacao"          : "Situacao",
        },
        key_cols=["CodigoCategoriaFinanceira"],   
        validation_rules={
            "CodigoCategoriaFinanceira" : {"dtype": str, "nullable": False, "unique": True},
            "IdCategoriaPai"            : {"dtype": str, "nullable": True},
            "Descricao"                 : {"dtype": str, "nullable": False},
            # "Descricao"                 : {"dtype": str, "nullable": False, "unique": True},
            "Tipo"                      : {"dtype": str},
            "Situacao"                  : {"dtype": str},
        }               
    ),

    "map_bling_contas_pagar": MappingSpec(
        src_to_tgt={
            "id"                    : "CodigoLancamento",
            "situacao"              : "Situacao",
            "vencimento"            : "DataVencimento",
            "valor"                 : "Valor",
            "contato.id"            : "CodContato",
            "formaPagamento.id"     : "CodFormaPagamento",
            "saldo"                 : "Saldo",
            "dataEmissao"           : "DataEmissao",
            "vencimentoOriginal"    : "DataVencimentoOriginal",
            "numeroDocumento"       : "NumeroDocumento",
            "competencia"           : "DataCompetencia",
            "historico"             : "Historico",
            "numeroBanco"           : "NumeroBanco",
            "portador.id"           : "CodPortador", 
            "categoria.id"          : "CodCategoria",
            "ocorrencia.tipo"       : "TipoOcorrencia",
        },
        key_cols=["CodigoLancamento"],
        validation_rules={
            "CodigoLancamento"          : {"nullable": False},
        }               
    ),

    "map_bling_contas_receber": MappingSpec(
        src_to_tgt={
            "id"                    : "CodigoLancamento",
            "situacao"              : "Situacao",
            "vencimento"            : "DataVencimento",
            "valor"                 : "Valor",
            "idTransacao"           : "CodTransacao",
            "linkQRCodePix"         : "LinkQRCodePix",
            "linkBoleto"            : "LinkBoleto",
            "dataEmissao"           : "DataEmissao",
            "contato.id"            : "CodContato",
            "formaPagamento.id"     : "CodFormaPagamento",
            "contaContabil.id"      : "CodContaContabil",
            "origem.id"             : "CodOrigem",
            "saldo"                 : "Saldo",
            "vencimentoOriginal"    : "DataVencimentoOriginal",
            "numeroDocumento"       : "NumeroDocumento",
            "competencia"           : "DataCompetencia",
            "historico"             : "Historico",
            "numeroBanco"           : "NumeroBanco",
            "portador.id"           : "CodPortador",
            "categoria.id"          : "CodCategoria",
            "vendedor.id"           : "CodVendedor",
            "ocorrencia.tipo"       : "TipoOcorrencia",
        },
        key_cols=["CodigoLancamento"],   
        validation_rules={
            "CodigoLancamento"          : {"dtype": str, "nullable": False, "unique": True},
        }               
    ),

    "map_bling_contas_contabeis": MappingSpec(
        src_to_tgt={
            "id"                    : "CodigoContaContabil",
            "descricao"             : "Descricao",
            "dataInicioTransacoes"  : "DataInicioTransacoes",
        },
        key_cols=["CodigoContaContabil"],   
        validation_rules={
            "CodigoContaContabil"          : {"dtype": str, "nullable": False, "unique": True},
        }               
    ),

    "map_bling_formas_pagamento": MappingSpec(
        src_to_tgt={
            "id"                : "CodigoFormaPagamento",
            "descricao"         : "Descricao",
            "tipoPagamento"     : "TipoPagamento",
            "situacao"          : "Situacao",
            "fixa"              : "Fixa",
            "padrao"            : "Padrao",
            "finalidade"        : "Finalidade",
            "juros"             : "Juros",
            "multa"             : "Multa",
            "condicao"          : "Condicao",
            "destino"           : "Destino",
            "utilizaDiasUteis"  : "UtilizaDiasUteis",
            "taxas.aliquota"    : "TaxaAliquota",
            "taxas.valor"       : "TaxaValor",
            "taxas.prazo"       : "TaxaPrazo",
        },
        key_cols=["CodigoFormaPagamento"],   
        validation_rules={
            "CodigoFormaPagamento"          : {"dtype": str, "nullable": False, "unique": True},
        }               
    ),
    
    "map_bling_nota_fiscal": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoNotaFiscal",
            "tipo"                          : "Tipo",
            "situacao"                      : "Situacao",
            "numero"                        : "Numero",
            "dataEmissao"                   : "DataEmissao",
            "dataOperacao"                  : "DataOperacao",
            "chaveAcesso"                   : "ChaveAcesso",
            "contato.id"                    : "CodContato",
            "naturezaOperacao.id"           : "CodNaturezaOperacao",
            "loja.id"                       : "CodLoja",
            "serie"                         : "Serie",
            "valorNota"                     : "ValorNota",
            "valorFrete"                    : "ValorFrete",
            "optanteSimplesNacional"        : "OptanteSimplesNacional",
            "numeroPedidoLoja"              : "NumeroPedidoLoja",
            "vendedor.id"                   : "CodVendedor",
            "transporte.fretePorConta"      : "TransporteFretePorConta",
            "transporte.transportador.nome" : "TransporteTransportadorNome",
            "etiqueta.nome"                 : "EtiquetaNome",
        },
        key_cols=["CodigoNotaFiscal"],   
        validation_rules={
            "CodigoNotaFiscal"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_nota_fiscal_item": MappingSpec(
        src_to_tgt={
            "id"                                            : "CodNotaFiscal",
            "itens.codigo"                                  : "CodigoProduto",
            "itens.descricao"                               : "Descricao",
            "itens.unidade"                                 : "Unidade",
            "itens.quantidade"                              : "Quantidade",
            "itens.valor"                                   : "Valor",
            "itens.valorTotal"                              : "ValorTotal",
            "itens.tipo"                                    : "Tipo",
            "itens.pesoBruto"                               : "PesoBruto",
            "itens.pesoLiquido"                             : "PesoLiquido",
            "itens.numeroPedidoCompra"                      : "NumeroPedidoCompra",
            "itens.classificacaoFiscal"                     : "ClassificacaoFiscal",
            "itens.cest"                                    : "Cest",
            "itens.codigoServico"                           : "CodigoServico",
            "itens.origem"                                  : "Origem",
            "itens.informacoesAdicionais"                   : "InformacoesAdicionais",
            "itens.gtin"                                    : "Gtin",
            "itens.cfop"                                    : "Cfop",
            "itens.impostos.valorAproximadoTotalTributos"   : "ImpostoValorAproximadoTotalTributos",
            "itens.impostos.icms"                           : "ImpostoIcmsSt",
            "itens.impostos.icms.st"                        : "ImpostoIcmsSt",
            "itens.impostos.icms.origem"                    : "ImpostoIcmsOrigem",
            "itens.impostos.icms.modalidade"                : "ImpostoIcmsModalidade",
            "itens.impostos.icms.aliquota"                  : "ImpostoIcmsAliquota",
            "itens.impostos.icms.valor"                     : "ImpostoIcmsValor",
        },
        key_cols=["CodNotaFiscal", "CodigoProduto", "Quantidade", "Valor"],   
        validation_rules={
            "CodNotaFiscal"          : {"nullable": False},
        }               
    ),

    "map_bling_nota_fiscal_parcela": MappingSpec(
        src_to_tgt={
            "id"                            : "CodNotaFiscal",
            "parcelas.data"                 : "DataParcela",
            "parcelas.valor"                : "ValorParcela",
            "parcelas.observacoes"          : "Observacoes",
            "parcelas.formaPagamento.id"    : "CodFormaPagamento",
        },
        key_cols=["CodNotaFiscal", "DataParcela", "ValorParcela"],   
        validation_rules={
            "CodNotaFiscal"          : {"nullable": False},
        }               
    ),

    #TODO - FInalizar
    "map_bling_produtos": MappingSpec(
        src_to_tgt={
            "id"                : "CodigoProduto",
            "nome"         : "Descricao",
            "codigo"     : "TipoPagamento",
            "preco"          : "Situacao",
            "estoque.minimo"              : "Fixa",
            "estoque.maximo"              : "Fixa",
            "estoque.crossdocking"              : "Fixa",
            "estoque.localizacao"              : "Fixa",
            "estoque.saldoVirtualTotal"              : "Fixa",
            "tipo"            : "Padrao",
            "situacao"        : "Finalidade",
            "formato"             : "Juros",
            "descricaoCurta"             : "Multa",
            "dataValidade"          : "Condicao",
            "unidade"           : "Destino",
            "pesoLiquido"  : "UtilizaDiasUteis",
            "pesoBruto"    : "TaxaAliquota",
            "volumes"       : "TaxaValor",
            "itensPorCaixa"       : "TaxaPrazo",
            "gtin"       : "TaxaPrazo",
            "gtinEmbalagem"       : "TaxaPrazo",
            "tipoProducao"       : "TaxaPrazo",
            "condicao"       : "TaxaPrazo",
            "freteGratis"       : "TaxaPrazo",
            "marca"       : "TaxaPrazo",
            "descricaoComplementar"       : "TaxaPrazo",
            "observacoes"       : "TaxaPrazo",
            "categoria.id"       : "TaxaPrazo",
        },
        key_cols=["CodigoFormaPagamento"],   
        validation_rules={
            "CodigoFormaPagamento"          : {"dtype": str, "nullable": False, "unique": True},
        }               
    ),



















}
