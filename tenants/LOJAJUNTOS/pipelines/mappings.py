from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandera.pandas as pa

@dataclass
class MappingSpec:
    """
    Define a estrutura de um mapeamento de dados.
    Este objeto contém todas as regras para traduzir, validar e carregar
    os dados de um job específico na Staging Area.
    """
    # Dicionário DE-PARA: {"Nome da Coluna na Origem": "Nome da Coluna no Destino"}
    src_to_tgt: Dict[str, str]

    # Lista de colunas que formam a chave primária de negócio.
    # Essencial para a lógica de MERGE (UPSERT) e deduplicação.
    key_cols: List[str]

    # (Opcional) Lista de colunas a serem comparadas para detectar atualizações no MERGE.
    # Se None, todas as colunas (exceto as key_cols) são comparadas.
    # Útil para otimizar a performance, ignorando colunas que não devem ser atualizadas.
    compare_cols: Optional[List[str]] = None

    # (Opcional) Regras de validação de dados usando a biblioteca Pandera.
    # O ETL falhará se os dados não passarem nessas validações.
    validation_rules: Dict[str, Dict] = field(default_factory=dict)


# ===================================================================
# MAPPINGS DEFINITIONS
# ===================================================================

# Dicionário principal que o orquestrador (main.py) irá consumir.
# A chave de cada entrada (ex: "clientes_api") deve corresponder ao `map_id`
# definido no arquivo `jobs.py`.

MAPPINGS = {

# ===================== CARGA STAGE ===================== #

    # =========== CATEGORIAS FINANCEIRAS =========== #
    "map_bling_categorias_financeiras": MappingSpec(
        src_to_tgt={
            "id"                : "CodigoCategoriaFinanceira",
            "idCategoriaPai"    : "CodCategoriaPai",
            "descricao"         : "Descricao",
            "tipo"              : "Tipo",
            "situacao"          : "Situacao",
        },
        key_cols=["CodigoCategoriaFinanceira"],   
        validation_rules={
            "CodigoCategoriaFinanceira" : {"nullable": False, "unique": True},
        }               
    ),

    # =========== CONTAS =========== #
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
            "CodigoLancamento"          : {"nullable": False, "unique": True}}               
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
            "CodigoLancamento"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_contas_receber_boleto": MappingSpec(
        src_to_tgt={
            "id"                    : "CodigoLancamento",
        },
        key_cols=["CodigoLancamento"],   
        validation_rules={
            "CodigoLancamento"          : {"nullable": False, "unique": True},
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
            "CodigoContaContabil"          : {"nullable": False, "unique": True},
        }               
    ),

    # =========== FORMAS PAGAMENTO =========== #
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
            "CodigoFormaPagamento"          : {"nullable": False, "unique": True},
        }               
    ),

    # =========== NOTA FISCAL =========== #
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
            "transporte.etiqueta.nome"      : "TransporteEtiquetaNome",
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
            "dataEmissao"                                   : "DataEmissao",
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
            "itens.impostos.icms.st"                        : "ImpostoIcmsSt",
            "itens.impostos.icms.origem"                    : "ImpostoIcmsOrigem",
            "itens.impostos.icms.modalidade"                : "ImpostoIcmsModalidade",
            "itens.impostos.icms.aliquota"                  : "ImpostoIcmsAliquota",
            "itens.impostos.icms.valor"                     : "ImpostoIcmsValor",
        },
        key_cols=["CodNotaFiscal", "DataEmissao", "CodigoProduto", "Quantidade", "Valor"],   
        validation_rules={
            "CodNotaFiscal"          : {"nullable": False},
        }               
    ),

    "map_bling_nota_fiscal_parcela": MappingSpec(
        src_to_tgt={
            "id"                            : "CodNotaFiscal",
            "dataEmissao"                   : "DataEmissao",
            "parcelas.data"                 : "DataParcela",
            "parcelas.valor"                : "ValorParcela",
            "parcelas.observacoes"          : "Observacoes",
            "parcelas.formaPagamento.id"    : "CodFormaPagamento",
        },
        key_cols=["CodNotaFiscal", "DataEmissao", "DataParcela", "ValorParcela"],   
        validation_rules={
            "CodNotaFiscal"          : {"nullable": False},
        }               
    ),

    # =========== NOTA FISCAL CONSUMIDOR =========== #
    "map_bling_nota_fiscal_consumidor": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoNotaFiscalConsumidor",
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
            "transporte.etiqueta.nome"      : "TransporteEtiquetaNome",
        },
        key_cols=["CodigoNotaFiscalConsumidor"],   
        validation_rules={
            "CodigoNotaFiscalConsumidor"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_nota_fiscal_consumidor_item": MappingSpec(
        src_to_tgt={
            "id"                                            : "CodigoNotaFiscalConsumidor",
            "itens.codigo"                                  : "CodigoProduto",
            "itens.descricao"                               : "Descricao",
            "dataEmissao"                                   : "DataEmissao",
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
            "itens.impostos.icms.st"                        : "ImpostoIcmsSt",
            "itens.impostos.icms.origem"                    : "ImpostoIcmsOrigem",
            "itens.impostos.icms.modalidade"                : "ImpostoIcmsModalidade",
            "itens.impostos.icms.aliquota"                  : "ImpostoIcmsAliquota",
            "itens.impostos.icms.valor"                     : "ImpostoIcmsValor",
        },
        key_cols=["CodigoNotaFiscalConsumidor", "DataEmissao", "CodigoProduto", "Quantidade", "Valor"],   
        validation_rules={
            "CodigoNotaFiscalConsumidor"          : {"nullable": False},
        }               
    ),

    "map_bling_nota_fiscal_consumidor_parcela": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoNotaFiscalConsumidor",
            "dataEmissao"                   : "DataEmissao",
            "parcelas.data"                 : "DataParcela",
            "parcelas.valor"                : "ValorParcela",
            "parcelas.observacoes"          : "Observacoes",
            "parcelas.formaPagamento.id"    : "CodFormaPagamento",
        },
        key_cols=["CodigoNotaFiscalConsumidor", "DataEmissao", "DataParcela", "ValorParcela"],   
        validation_rules={
            "CodigoNotaFiscalConsumidor"          : {"nullable": False},
        }               
    ),

    # =========== PRODUTOS =========== #
    "map_bling_produtos": MappingSpec(
        src_to_tgt={
            "id"                        : "CodigoProduto",
            "nome"                      : "Nome",
            "codigo"                    : "CodigoSKU",
            "preco"                     : "Preco",
            "estoque.minimo"            : "EstoqueMinimo",
            "estoque.maximo"            : "EstoqueMaximo",
            "estoque.crossdocking"      : "EstoqueCrossdocking",
            "estoque.localizacao"       : "EstoqueLocalizacao",
            "estoque.saldoVirtualTotal" : "EstoqueSaldoVirtualTotal",
            "tipo"                      : "Tipo",
            "situacao"                  : "Situacao",
            "formato"                   : "Formato",
            "descricaoCurta"            : "DescricaoCurta",
            "dataValidade"              : "DataValidade",
            "unidade"                   : "Unidade",
            "pesoLiquido"               : "PesoLiquido",
            "pesoBruto"                 : "PesoBruto",
            "volumes"                   : "Volumes",
            "itensPorCaixa"             : "ItensPorCaixa",
            "gtin"                      : "Gtin",
            "gtinEmbalagem"             : "GtinEmbalagem",
            "tipoProducao"              : "TipoProducao",
            "condicao"                  : "Condicao",
            "freteGratis"               : "FreteGratis",
            "marca"                     : "Marca",
            "descricaoComplementar"     : "DescricaoComplementar",
            "observacoes"               : "Observacoes",
            "categoria.id"              : "CodCategoria",
            "tributacao.grupoProduto"   : "GrupoProduto",
        },
        key_cols=["CodigoProduto"],   
        validation_rules={
            "CodigoProduto"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_produto_tributacao": MappingSpec(
        src_to_tgt={
            "id"                            : "Codroduto",
            "tributacao.origem"                        : "Origem",
            "tributacao.nFCI"                          : "nFCI",
            "tributacao.ncm"                           : "NCM",
            "tributacao.cest"                          : "CEST",
            "tributacao.codigoListaServicos"           : "CodigoListaServicos",
            "tributacao.spedTipoItem"                  : "SpedTipoItem",
            "tributacao.codigoItem"                    : "CodigoItem",
            "tributacao.percentualTributos"            : "PercentualTributos",
            "tributacao.valorBaseStRetencao"           : "ValorBaseStRetencao",
            "tributacao.valorStRetencao"               : "ValorStRetencao",
            "tributacao.valorICMSSubstituto"           : "ValorICMSSubstituto",
            "tributacao.codigoExcecaoTipi"             : "CodigoExcecaoTipi",
            "tributacao.classeEnquadramentoIpi"        : "ClasseEnquadramentoIpi",
            "tributacao.valorIpiFixo"                  : "ValorIpiFixo",
            "tributacao.codigoSeloIpi"                 : "CodigoSeloIpi",
            "tributacao.valorPisFixo"                  : "ValorPisFixo",
            "tributacao.valorCofinsFixo"               : "ValorCofinsFixo",
            "tributacao.codigoANP"                     : "CodigoANP",
            "tributacao.descricaoANP"                  : "DescricaoANP",
            "tributacao.percentualGLP"                 : "PercentualGLP",
            "tributacao.percentualGasNacional"         : "PercentualGasNacional",
            "tributacao.percentualGasImportado"        : "PercentualImportado",
            "tributacao.valorPartida"                  : "ValorPartida",
            "tributacao.tipoArmamento"                 : "TipoArmamento",
            "tributacao.descricaoCompletaArmamento"    : "DescricaoCompletaArmamento",
            "tributacao.dadosAdicionais"               : "DadosAdicionais",
            "tributacao.grupoProduto"                  : "GrupoProduto",
        },
        key_cols=["Codroduto"],   
        validation_rules={
            "Codroduto"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_produto_grupo": MappingSpec(
        src_to_tgt={
            "id"                    : "CodigoGrupoProduto",
            "nome"                  : "Nome",
            "grupoProdutoPai.id"    : "CodGrupoProdutoPai",
        },
        key_cols=["CodigoGrupoProduto"],   
        validation_rules={
            "CodigoGrupoProduto"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_produto_categoria": MappingSpec(
        src_to_tgt={
            "id"            : "CodigoCategoriaProduto",
            "descricao"     : "Descricao",
            "ProdutoPai.id" : "CodCategoriaProdutoPai",
        },
        key_cols=["CodigoCategoriaProduto"],   
        validation_rules={
            "CodigoCategoriaProduto"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_produto_fornecedor": MappingSpec(
        src_to_tgt={
            "id"            : "CodigoProdutoFornecedor",
            "descricao"     : "Descricao",
            "codigo"        : "Codigo",
            "precoCusto"    : "PrecoCusto",
            "precoCompra"   : "PrecoCompra",
            "padrao"        : "Padrao",
            "produto.id"    : "CodProduto",
            "fornecedor.id" : "CodFornecedor",
            "garantia"      : "Garantia",
        },
        key_cols=["CodigoProdutoFornecedor"],   
        validation_rules={
            "CodigoProdutoFornecedor"          : {"nullable": False, "unique": True},
        }               
    ),

    # =========== CONTATO =========== #
   "map_bling_contato": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoContato",
            "nome"                          : "Nome",
            "codigo"                        : "Codigo",
            "situacao"                      : "Situacao",
            "numeroDocumento"               : "NumeroDocumento",
            "telefone"                      : "Telefone",
            "celular"                       : "Celular",
            "fantasia"                      : "NomeFantasia",
            "tipo"                          : "Tipo",
            "indicadorIe"                   : "IndicadorIe",
            "ie"                            : "ie",
            "rg"                            : "rg",
            "inscricaoMunicipal"            : "InscricaoMunicipal",
            "orgaoEmissor"                  : "OrgaoEmissor",
            "email"                         : "Email",
            "emailNotaFiscal"               : "EmailNotaFiscal",
            "endereco.geral.endereco"       : "Endereco",
            "endereco.geral.cep"            : "CEP",
            "endereco.geral.bairro"         : "Bairro",
            "endereco.geral.municipio"      : "Municipio",
            "endereco.geral.uf"             : "UF",
            "endereco.geral.numero"         : "Numero",
            "endereco.geral.complemento"    : "Complemento",
            "tiposContato.id"               : "CodTipoContato",
        },
        key_cols=["CodigoContato"],   
        validation_rules={
            "CodigoContato"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_tipo_contato": MappingSpec(
        src_to_tgt={
            "id"        : "CodigoTipoContato",
            "descricao" : "Descricao",
        },
        key_cols=["CodigoTipoContato"],   
        validation_rules={
            "CodigoTipoContato"          : {"nullable": False, "unique": True},
        }               
    ),

   "map_bling_contato_consumidor_final": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoContato",
            "nome"                          : "Nome",
            "codigo"                        : "Codigo",
            "situacao"                      : "Situacao",
            "numeroDocumento"               : "NumeroDocumento",
            "telefone"                      : "Telefone",
            "celular"                       : "Celular",
            "fantasia"                      : "NomeFantasia",
            "tipo"                          : "Tipo",
            "indicadorIe"                   : "IndicadorIe",
            "ie"                            : "ie",
            "rg"                            : "rg",
            "inscricaoMunicipal"            : "InscricaoMunicipal",
            "orgaoEmissor"                  : "OrgaoEmissor",
            "email"                         : "Email",
            "emailNotaFiscal"               : "EmailNotaFiscal",
            "endereco.geral.endereco"       : "Endereco",
            "endereco.geral.cep"            : "CEP",
            "endereco.geral.bairro"         : "Bairro",
            "endereco.geral.municipio"      : "Municipio",
            "endereco.geral.uf"             : "UF",
            "endereco.geral.numero"         : "Numero",
            "endereco.geral.complemento"    : "Complemento",
            "tiposContato.id"               : "CodTipoContato",
        },
        key_cols=["CodigoContato"],   
        validation_rules={
            "CodigoContato"          : {"nullable": False, "unique": True},
        }               
    ),









}
