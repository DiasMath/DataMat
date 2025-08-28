-- Garante que a versão mais recente da VIEW será criada.
DROP VIEW IF EXISTS HASHTAG_STG.vwSTG_DREFormatos;

CREATE VIEW HASHTAG_STG.vwSTG_DREFormatos AS
SELECT
    Ordem,
    Grupo,
    CASE
        WHEN Subtotal = 1 THEN 'ST' -- Subtotal
        WHEN Subtotal = 0 THEN 'A'  -- Analítico
        ELSE 'Indefinido'
    END AS Tipo
FROM
    HASHTAG_STG.tbSTG_DREFormatos;