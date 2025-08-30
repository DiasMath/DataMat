SELECT
    nome_da_coluna,
    COUNT(*) AS quantidade
FROM
    nome_da_tabela
GROUP BY
    nome_da_coluna
HAVING
    COUNT(*) > 1;