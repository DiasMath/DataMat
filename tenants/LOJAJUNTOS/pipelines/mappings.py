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

    # Caminho para a lista a ser "explodida" em múltiplas linhas (ex: 'itens')
    record_path: Optional[str] = None
    
    # Colunas do nível superior (cabeçalho) a serem mantidas em cada linha explodida
    meta_cols: Optional[List[str]] = None

    # Prefixo para colunas de metadados para evitar conflitos de nome.
    meta_prefix: Optional[str] = None


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
            "portador.id"           : "CodContaContabil", 
            "categoria.id"          : "CodCategoriaFinanceira",
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
            "origem.id"             : "CodNotaFiscalOrigem",
            "saldo"                 : "Saldo",
            "vencimentoOriginal"    : "DataVencimentoOriginal",
            "numeroDocumento"       : "NumeroDocumento",
            "competencia"           : "DataCompetencia",
            "historico"             : "Historico",
            "numeroBanco"           : "NumeroBanco",
            "categoria.id"          : "CodCategoriaFinanceira",
            "vendedor.id"           : "CodVendedor",
            "ocorrencia.tipo"       : "TipoOcorrencia",
        },
        key_cols=["CodigoLancamento"],   
        validation_rules={
            "CodigoLancamento"          : {"nullable": False, "unique": True},
        }               
    ),

    # "map_bling_contas_receber_boleto": MappingSpec(
    #     src_to_tgt={
    #         "id"                    : "CodigoLancamento",
    #     },
    #     key_cols=["CodigoLancamento"],   
    #     validation_rules={
    #         "CodigoLancamento"          : {"nullable": False, "unique": True},
    #     }               
    # ),

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

        # Temporário enquanto o Filipe não resolve a questão de baixar as contas R e P das contas a pagar e receber.
        "temp_map_bling_file_contas_pagar_e_receber": MappingSpec(
        src_to_tgt={
            "Id"                    : "CodigoMovimentacaoFinanceiraOrigem",
            "Data"                  : "DataCompetencia",
            "Cliente/Fornecedor"    : "NomeContato",
            "CPF/CNPJ"              : "CPF/CNPJ",
            "Categoria"             : "Categoria",
            "Histórico"             : "Historico",
            "Tipo"                  : "Tipo",
            "Valor"                 : "Valor",
            "Banco"                 : "Banco",
            "Período"               : "PeriodoExtracao",
        },
        key_cols=["CodigoMovimentacaoFinanceiraOrigem"],
        validation_rules={ "CodigoMovimentacaoFinanceiraOrigem" : {"nullable": False, "unique": True}}               
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
            "id"                                      : "CodNotaFiscal",
            "codigo"                                  : "CodigoSKU",
            "descricao"                               : "Descricao",
            "dataEmissao"                             : "DataEmissao",
            "unidade"                                 : "Unidade",
            "quantidade"                              : "Quantidade",
            "valor"                                   : "Valor",
            "valorTotal"                              : "ValorTotal",
            "tipo"                                    : "Tipo",
            "pesoBruto"                               : "PesoBruto",
            "pesoLiquido"                             : "PesoLiquido",
            "numeroPedidoCompra"                      : "NumeroPedidoCompra",
            "classificacaoFiscal"                     : "ClassificacaoFiscal",
            "cest"                                    : "Cest",
            "codigoServico"                           : "CodigoServico",
            "origem"                                  : "Origem",
            "informacoesAdicionais"                   : "InformacoesAdicionais",
            "gtin"                                    : "Gtin",
            "cfop"                                    : "Cfop",
            "impostos.valorAproximadoTotalTributos"   : "ImpostoValorAproximadoTotalTributos",
            "impostos.icms.st"                        : "ImpostoIcmsSt",
            "impostos.icms.origem"                    : "ImpostoIcmsOrigem",
            "impostos.icms.modalidade"                : "ImpostoIcmsModalidade",
            "impostos.icms.aliquota"                  : "ImpostoIcmsAliquota",
            "impostos.icms.valor"                     : "ImpostoIcmsValor",
        },
        key_cols=["CodNotaFiscal", "CodigoSKU", "Descricao", 
                  "DataEmissao", "Unidade", "Quantidade", "Valor", "ValorTotal", "Tipo", "PesoBruto", "PesoLiquido",
                  "NumeroPedidoCompra", "ClassificacaoFiscal", "Cest", "CodigoServico", "Origem",
                  "InformacoesAdicionais", "Gtin", "Cfop", "ImpostoValorAproximadoTotalTributos",
                  "ImpostoIcmsSt", "ImpostoIcmsOrigem", "ImpostoIcmsModalidade", "ImpostoIcmsAliquota", "ImpostoIcmsValor" ],   
        record_path="itens",
        meta_cols=["id", "dataEmissao"],
        validation_rules={
            "CodNotaFiscal"          : {"nullable": False},
        }               
    ),

    # "map_bling_nota_fiscal_parcela": MappingSpec(
    #     src_to_tgt={
    #         "id"                   : "CodNotaFiscal",
    #         "dataEmissao"          : "DataEmissao",
    #         "data"                 : "DataParcela",
    #         "valor"                : "ValorParcela",
    #         "observacoes"          : "Observacoes",
    #         "formaPagamento.id"    : "CodFormaPagamento",
    #     },
    #     key_cols=["CodNotaFiscal", "DataEmissao", "DataParcela", "ValorParcela", "Observacoes", "CodFormaPagamento"],   
    #     record_path="parcelas",
    #     meta_cols=["id", "dataEmissao"],
    #     validation_rules={
    #         "CodNotaFiscal"          : {"nullable": False},
    #     }               
    # ),

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
            "id"                                      : "CodigoNotaFiscalConsumidor",
            "codigo"                                  : "CodigoSKU",
            "descricao"                               : "Descricao",
            "dataEmissao"                             : "DataEmissao",
            "unidade"                                 : "Unidade",
            "quantidade"                              : "Quantidade",
            "valor"                                   : "Valor",
            "valorTotal"                              : "ValorTotal",
            "tipo"                                    : "Tipo",
            "pesoBruto"                               : "PesoBruto",
            "pesoLiquido"                             : "PesoLiquido",
            "numeroPedidoCompra"                      : "NumeroPedidoCompra",
            "classificacaoFiscal"                     : "ClassificacaoFiscal",
            "cest"                                    : "Cest",
            "codigoServico"                           : "CodigoServico",
            "origem"                                  : "Origem",
            "informacoesAdicionais"                   : "InformacoesAdicionais",
            "gtin"                                    : "Gtin",
            "cfop"                                    : "Cfop",
            "impostos.valorAproximadoTotalTributos"   : "ImpostoValorAproximadoTotalTributos",
            "impostos.icms.st"                        : "ImpostoIcmsSt",
            "impostos.icms.origem"                    : "ImpostoIcmsOrigem",
            "impostos.icms.modalidade"                : "ImpostoIcmsModalidade",
            "impostos.icms.aliquota"                  : "ImpostoIcmsAliquota",
            "impostos.icms.valor"                     : "ImpostoIcmsValor",
        },
        key_cols=["CodigoNotaFiscalConsumidor", "CodigoSKU", "Descricao", 
                  "DataEmissao", "Unidade", "Quantidade", "Valor", "ValorTotal", "Tipo", "PesoBruto", "PesoLiquido",
                  "NumeroPedidoCompra", "ClassificacaoFiscal", "Cest", "CodigoServico", "Origem",
                  "InformacoesAdicionais", "Gtin", "Cfop", "ImpostoValorAproximadoTotalTributos",
                  "ImpostoIcmsSt", "ImpostoIcmsOrigem", "ImpostoIcmsModalidade", "ImpostoIcmsAliquota", "ImpostoIcmsValor" ],
        record_path="itens",
        meta_cols=["id", "dataEmissao"],   
        validation_rules={
            "CodigoNotaFiscalConsumidor"          : {"nullable": False},
        }               
    ),

    # "map_bling_nota_fiscal_consumidor_parcela": MappingSpec(
    #     src_to_tgt={
    #         "id"                   : "CodigoNotaFiscalConsumidor",
    #         "dataEmissao"          : "DataEmissao",
    #         "data"                 : "DataParcela",
    #         "valor"                : "ValorParcela",
    #         "observacoes"          : "Observacoes",
    #         "formaPagamento.id"    : "CodFormaPagamento",
    #     },
    #     key_cols=["CodigoNotaFiscalConsumidor", "DataEmissao", "DataParcela", 
    #               "ValorParcela", "Observacoes", "CodFormaPagamento"],   
    #     record_path="parcelas",
    #     meta_cols=["id", "dataEmissao"],
    #     validation_rules={
    #         "CodigoNotaFiscalConsumidor"          : {"nullable": False},
    #     }               
    # ),

    # =========== PRODUTOS =========== #
    "map_bling_produtos": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoProduto",
            "nome"                          : "Nome",
            "codigo"                        : "CodigoSKU",
            "preco"                         : "Preco",
            "estoque.minimo"                : "EstoqueMinimo",
            "estoque.maximo"                : "EstoqueMaximo",
            "estoque.crossdocking"          : "EstoqueCrossdocking",
            "estoque.localizacao"           : "EstoqueLocalizacao",
            "estoque.saldoVirtualTotal"     : "EstoqueSaldoVirtualTotal",
            "tipo"                          : "Tipo",
            "situacao"                      : "Situacao",
            "formato"                       : "Formato",
            "descricaoCurta"                : "DescricaoCurta",
            "dataValidade"                  : "DataValidade",
            "unidade"                       : "Unidade",
            "pesoLiquido"                   : "PesoLiquido",
            "pesoBruto"                     : "PesoBruto",
            "volumes"                       : "Volumes",
            "itensPorCaixa"                 : "ItensPorCaixa",
            "gtin"                          : "Gtin",
            "gtinEmbalagem"                 : "GtinEmbalagem",
            "tipoProducao"                  : "TipoProducao",
            "condicao"                      : "Condicao",
            "freteGratis"                   : "FreteGratis",
            "marca"                         : "Marca",
            "descricaoComplementar"         : "DescricaoComplementar",
            "observacoes"                   : "Observacoes",
            "categoria.id"                  : "CodCategoria",
            "tributacao.grupoProduto.id"    : "GrupoProduto",
        },
        key_cols=["CodigoProduto"],   
        validation_rules={
            "CodigoProduto"          : {"nullable": False, "unique": True},
        }               
    ),

    # "map_bling_produto_tributacao": MappingSpec(
    #     src_to_tgt={
    #         "id"                            : "Codroduto",
    #         "tributacao.origem"                        : "Origem",
    #         "tributacao.nFCI"                          : "nFCI",
    #         "tributacao.ncm"                           : "NCM",
    #         "tributacao.cest"                          : "CEST",
    #         "tributacao.codigoListaServicos"           : "CodigoListaServicos",
    #         "tributacao.spedTipoItem"                  : "SpedTipoItem",
    #         "tributacao.codigoItem"                    : "CodigoItem",
    #         "tributacao.percentualTributos"            : "PercentualTributos",
    #         "tributacao.valorBaseStRetencao"           : "ValorBaseStRetencao",
    #         "tributacao.valorStRetencao"               : "ValorStRetencao",
    #         "tributacao.valorICMSSubstituto"           : "ValorICMSSubstituto",
    #         "tributacao.codigoExcecaoTipi"             : "CodigoExcecaoTipi",
    #         "tributacao.classeEnquadramentoIpi"        : "ClasseEnquadramentoIpi",
    #         "tributacao.valorIpiFixo"                  : "ValorIpiFixo",
    #         "tributacao.codigoSeloIpi"                 : "CodigoSeloIpi",
    #         "tributacao.valorPisFixo"                  : "ValorPisFixo",
    #         "tributacao.valorCofinsFixo"               : "ValorCofinsFixo",
    #         "tributacao.codigoANP"                     : "CodigoANP",
    #         "tributacao.descricaoANP"                  : "DescricaoANP",
    #         "tributacao.percentualGLP"                 : "PercentualGLP",
    #         "tributacao.percentualGasNacional"         : "PercentualGasNacional",
    #         "tributacao.percentualGasImportado"        : "PercentualImportado",
    #         "tributacao.valorPartida"                  : "ValorPartida",
    #         "tributacao.tipoArmamento"                 : "TipoArmamento",
    #         "tributacao.descricaoCompletaArmamento"    : "DescricaoCompletaArmamento",
    #         "tributacao.dadosAdicionais"               : "DadosAdicionais",
    #         "tributacao.grupoProduto"                  : "GrupoProduto",
    #     },
    #     key_cols=["Codroduto"],   
    #     validation_rules={
    #         "Codroduto"          : {"nullable": False, "unique": True},
    #     }               
    # ),

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
            "id"                : "CodigoCategoriaProduto",
            "descricao"         : "Descricao",
            "categoriaPai.id"   : "CodCategoriaProdutoPai",
        },
        key_cols=["CodigoCategoriaProduto"],   
        validation_rules={
            "CodigoCategoriaProduto"          : {"nullable": False, "unique": True},
        }               
    ),

    # "map_bling_produto_fornecedor": MappingSpec(
    #     src_to_tgt={
    #         "id"            : "CodigoProdutoFornecedor",
    #         "descricao"     : "Descricao",
    #         "codigo"        : "Codigo",
    #         "precoCusto"    : "PrecoCusto",
    #         "precoCompra"   : "PrecoCompra",
    #         "padrao"        : "Padrao",
    #         "produto.id"    : "CodProduto",
    #         "fornecedor.id" : "CodFornecedor",
    #         "garantia"      : "Garantia",
    #     },
    #     key_cols=["CodigoProdutoFornecedor"],   
    #     validation_rules={
    #         "CodigoProdutoFornecedor"          : {"nullable": False, "unique": True},
    #     }               
    # ),

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
            "tipo"                          : "TipoPessoa",
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
        },
        key_cols=["CodigoContato"],   
        validation_rules={
            "CodigoContato"          : { "unique": True},
        }               
    ),

    "map_bling_contato_tipos_contato": MappingSpec(
        src_to_tgt={
            "contato_id"    : "CodContato",
            "id"            : "CodTipoContato" 
        },
        key_cols=["CodContato", "CodTipoContato"],
        record_path="tiposContato",
        meta_cols=["id"],
        meta_prefix="contato_",
        validation_rules={
            "CodContato"      : {"nullable": False},
            "CodTipoContato"  : {"nullable": False},
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
            "tipo"                          : "TipoPessoa",
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
        },
        key_cols=["CodigoContato"],   
        validation_rules={
            "CodigoContato"          : {"nullable": False, "unique": True},
        }               
    ),

    # =========== NATUREZA OPERACAO =========== #
   "map_bling_natureza_operacao": MappingSpec(
        src_to_tgt={
            "id"        : "CodigoNaturezaOperacao",
            "situacao"  : "Situacao",
            "padrao"    : "Padrao",
            "descricao" : "Descricao",
        },
        key_cols=["CodigoNaturezaOperacao"],   
        validation_rules={
            "CodigoNaturezaOperacao"          : {"nullable": False, "unique": True},
        }               
    ),

    # =========== VENDEDORES =========== #
   "map_bling_vendedores": MappingSpec(
        src_to_tgt={
            "id"                : "CodigoVendedor",
            "descontoLimite"    : "DescontoLimite",
            "loja.id"           : "CodLoja",
            "descontoMaximo"    : "ComissaoDescontoMaximo",
            "aliquota"          : "ComissaoAliquota",
            "contato.id"        : "CodContato",
        },
        key_cols=["CodigoVendedor"],   
        record_path="comissoes",
        meta_cols=["id", "descontoLimite", "loja.id", "contato.id"],
        validation_rules={
            "CodigoVendedor"          : {"nullable": False, "unique": True},
        }               
    ),

   # =========== PEDIDO =========== #
   "map_bling_pedido_compra": MappingSpec(
        src_to_tgt={
            "id"                            : "CodigoPedidoCompra",
            "numero"                        : "Numero",
            "data"                          : "DataPedidoCompra",
            "dataPrevista"                  : "DataPrevista",
            "totalProdutos"                 : "TotalProdutos",
            "total"                         : "Total",
            "fornecedor.id"                 : "CodContatoFornecedor",
            "situacao.valor"                : "SituacaoValor",
            "ordemCompra"                   : "OrdemCompra",
            "observacoes"                   : "Observacoes",
            "observacoesInternas"           : "ObservacoesInternas",
            "desconto.valor"                : "DescontoValor",
            "desconto.unidade"              : "DescontoUnidade",
            "categoria.id"                  : "CodCategoriaFinanceira",
            "tributacao.totalICMS"          : "TributacaoTotalICMS",
            "tributacao.totalIPI"           : "TributacaoTotalIPI",
            "transporte.frete"              : "TransporteFrete",
            "transporte.transportador"      : "TransporteTransportador",
            "transporte.fretePorConta"      : "TransporteFretePorConta",
            "transporte.pesoBruto"          : "TransportePesoBruto",
            "transporte.volumes"            : "TransporteVolumes",
        },
        key_cols=["CodigoPedidoCompra"],   
        validation_rules={
            "CodigoPedidoCompra"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_pedido_compra_item": MappingSpec(
        src_to_tgt={
            "id"                        : "CodigoPedidoCompra",
            "data"                      : "DataPedidoCompra",
            "descricao"                 : "Descricao",
            "codigoFornecedor"          : "CodigoProdutoFornecedor",
            "unidade"                   : "Unidade",
            "valor"                     : "Valor",
            "quantidade"                : "Quantidade",
            "aliquotaIPI"               : "AliquotaIPI",
            "descricaoDetalhada"        : "DescricaoDetalhada",
            "notaFiscal.id"             : "CodNotaFiscal",
            "notaFiscal.quantidade"     : "NotaFiscalQuantidade",
            "produto.id"                : "CodProduto",
        },
        key_cols=["CodigoPedidoCompra", "DataPedidoCompra", "Descricao", "CodigoProdutoFornecedor", "Unidade",
                  "Valor", "Quantidade", "AliquotaIPI", "DescricaoDetalhada", "CodNotaFiscal", "NotaFiscalQuantidade",
                  "CodProduto"],  
        record_path="itens",
        meta_cols=["id", "data"], 
        validation_rules={
            "CodigoPedidoCompra"          : {"nullable": False},
        }               
    ),

    "map_bling_pedido_venda": MappingSpec(
        src_to_tgt={
            "id"                                : "CodigoPedidoVenda",
            "numero"                            : "Numero",
            "numeroLoja"                        : "NumeroLoja",
            "data"                              : "DataPedidoVenda",
            "dataSaida"                         : "DataSaida",
            "dataPrevista"                      : "DataPrevista",
            "totalProdutos"                     : "TotalProdutos",
            "total"                             : "Total",
            "contato.id"                        : "CodContato",
            "situacao.id"                       : "CodSituacao",
            "situacao.valor"                    : "SituacaoValor",
            "loja.id"                           : "CodLoja",
            "numeroPedidoCompra"                : "NumeroPedidoCompra",
            "outrasDespesas"                    : "OutrasDespesas",
            "observacoes"                       : "Observacoes",
            "observacoesInternas"               : "ObservacoesInternas",
            "desconto.valor"                    : "DescontoValor",
            "desconto.unidade"                  : "DescontoUnidade",
            "categoria.id"                      : "CodCategoriaFinanceira",
            "notaFiscal.id"                     : "CodNotaFiscal",
            "tributacao.totalICMS"              : "TributacaoTotalICMS",
            "tributacao.totalIPI"               : "TributacaoTotalIPI",
            "transporte.fretePorConta"          : "TransporteFretePorConta",
            "transporte.quantidadeVolumes"      : "TransporteQuantidadeVolumes",
            "transporte.prazoEntrega"           : "TransportePrazoEntrega",
            "transporte.contato.id"             : "TransporteCodContato",
            "transporte.etiqueta.nome"          : "TransporteEtiquetaNome",
            "vendedor.id"                       : "CodVendedor",
            "intermediador.cnpj"                : "IntermediadorCNPJ",
            "intermediador.nomeUsuario"         : "IntermediadorNomeUsuario",
            "taxas.taxaComissao"                : "TaxaComissao",
            "taxas.custoFrete"                  : "TaxaCustoFrete",
            "taxas.valorBase"                   : "TaxaValorBase",
        },
        key_cols=["CodigoPedidoVenda"],  
        validation_rules={
            "CodigoPedidoVenda"          : {"nullable": False, "unique": True},
        }               
    ),

    "map_bling_pedido_venda_item": MappingSpec(
        src_to_tgt={
            "pedido_venda_item_id"      : "CodigoPedidoVenda",
            "id"                        : "CodigoPedidoVendaItem",
            "pedido_venda_item_data"    : "DataPedidoVenda",
            "codigo"                    : "CodigoSKU",
            "unidade"                   : "Unidade",
            "quantidade"                : "Quantidade",
            "desconto"                  : "Desconto",
            "valor"                     : "Valor",
            "aliquotaIPI"               : "AliquotaIPI",
            "descricao"                 : "Descricao",
            "descricaoDetalhada"        : "DescricaoDetalhada",
            "produto.id"                : "CodProduto",
            "comissao.base"             : "ComissaoBase",
            "comissao.aliquota"         : "ComissaoAliquota",
            "comissao.valor"            : "ComissaoValor",
        },
        key_cols=["CodigoPedidoVendaItem", "CodigoSKU", "Unidade", "Quantidade", "Desconto", "Valor", "AliquotaIPI",
                   "Descricao", "DescricaoDetalhada", "CodProduto", "ComissaoBase", "ComissaoAliquota", "ComissaoValor"], 
        record_path="itens",
        meta_cols=["id", "data"],
        meta_prefix="pedido_venda_item_", 
        validation_rules={
            "CodigoPedidoVenda"          : {"nullable": False},
        }               
    ),

    # "map_bling_pedido_venda_parcela": MappingSpec(
    #     src_to_tgt={
    #         "id"                        : "CodigoPedidoVenda",
    #         "pedido_venda_parcela_id"   : "CodigoPedidoVendaParcela",
    #         "pedido_venda_parcela_data" : "DataPedidoVenda",
    #         "dataVencimento"            : "DataVencimento",
    #         "valor"                     : "Valor",
    #         "observacoes"               : "Observacoes",
    #         "formaPagamento.id"         : "CodFormaPagamento",
    #     },
    #     key_cols=["CodigoPedidoVendaParcela", "DataPedidoVenda", "DataVencimento", 
    #               "Valor", "Observacoes", "CodFormaPagamento"], 
    #     record_path="parcelas",
    #     meta_cols=["id", "data"], 
    #     meta_prefix="pedido_venda_parcela_", 
    #     validation_rules={
    #         "CodigoPedidoVenda"          : {"nullable": False},
    #     }               
    # ),
}
