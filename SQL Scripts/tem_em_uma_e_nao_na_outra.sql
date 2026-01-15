-- Template: Registros que estão em A, mas não em B
SELECT
    A.* -- Seleciona todas as colunas da tabela A
FROM
    LOJAJUNTOS_STG.tbSTG_BLING_FILE_MovimentacoesFinanceiras AS A
LEFT JOIN
    tbFatoMovimentacoesFinanceiras_DRIVE AS B
    -- Aqui você DEVE definir as chaves de ligação
    ON A.CodigoMovimentacaoFinanceiraOrigem = B.CodLancamentoOrigem
    -- Se for uma chave composta (múltiplas colunas), adicione mais condições:
    -- AND A.outra_chave_parte1 = B.outra_chave_parte1
    -- AND A.outra_chave_parte2 = B.outra_chave_parte2
WHERE
    -- Filtra apenas onde a chave da Tabela B for NULA,
    -- indicando que não houve correspondência.
    B.CodLancamentoOrigem IS NULL;