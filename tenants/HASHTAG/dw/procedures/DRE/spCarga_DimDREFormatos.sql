-- =================================================================================
-- TEMPLATE PARA PROCEDURE DE CARGA INCREMENTAL (UPSERT)
--
-- INSTRUÇÕES DE USO:
-- 1. Copie este arquivo e renomeie para "spCarga_Dim<SuaTabela>.sql".
-- 2. Use a função "Localizar e Substituir" do seu editor para trocar os 
--    placeholders abaixo pelos valores corretos.
--
-- PLACEHOLDERS:
-- __NOME_PROCEDURE__     -> Ex: spCarga_DimCliente
-- __SCHEMA_DW__          -> Ex: HASHTAG_DW
-- __TABELA_DW__          -> Ex: tbDim_Cliente
-- __SCHEMA_STG__         -> Ex: HASHTAG_STG
-- __TABELA_STG__         -> Ex: tbSTG_Cliente ou View
-- __CHAVE_NEGOCIO__      -> Coluna para o JOIN. Ex: ClienteID
-- __COLUNAS_UPDATE__     -> Lista de colunas para o SET. Ex: dw.Nome = stg.Nome, dw.Cidade = stg.Cidade
-- __COLUNAS_INSERT__     -> Lista de colunas para o INSERT. Ex: ClienteID, Nome, Cidade
-- __VALORES_INSERT__     -> Lista de valores do SELECT. Ex: stg.ClienteID, stg.Nome, stg.Cidade
-- __COLUNAS_WHERE__      -> Condições para o WHERE do UPDATE. Ex: dw.Nome <=> stg.Nome IS NOT TRUE OR dw.Cidade <=> stg.Cidade IS NOT TRUE
-- =================================================================================

DROP PROCEDURE IF EXISTS spCarga_DimDREFormatos;

DELIMITER //

CREATE PROCEDURE spCarga_DimDREFormatos(
    OUT p_inserted_rows INT,
    OUT p_updated_rows INT
)
BEGIN
    -- REMOVIDO: O bloco "DECLARE EXIT HANDLER" foi removido.
    -- A responsabilidade de capturar a exceção e fazer o ROLLBACK
    -- agora é 100% do orquestrador Python.

    -- A procedure agora só contém a lógica de negócio.
    -- Se qualquer comando aqui falhar, a exceção irá parar a execução
    -- e será propagada para a aplicação que a chamou.

    -- =================================================================================
    -- PASSO 1: ATUALIZAR REGISTOS EXISTENTES E ALTERADOS (MERGE - UPDATE)
    -- =================================================================================
    UPDATE
        HASHTAG_DW.tbDim_DREFormatos AS dw
    JOIN
        HASHTAG_STG.vwSTG_DREFormatos AS stg ON dw.Grupo = stg.Grupo
    SET
        dw.Ordem = stg.Ordem,
        dw.Tipo  = stg.Tipo 
    WHERE
        dw.Ordem <=> stg.Ordem IS NOT TRUE
        OR dw.Tipo <=> stg.Tipo IS NOT TRUE;

    SET p_updated_rows = ROW_COUNT();

    -- =================================================================================
    -- PASSO 2: INSERIR NOVOS REGISTOS (MERGE - INSERT)
    -- =================================================================================
    INSERT INTO
        HASHTAG_DW.tbDim_DREFormatos (Ordem, Grupo, Tipo)
    SELECT
        stg.Ordem,
        stg.Grupo,
        stg.Tipo
    FROM
        HASHTAG_STG.vwSTG_DREFormatos AS stg
    LEFT JOIN
        HASHTAG_DW.tbDim_DREFormatos AS dw 
            ON stg.Grupo = dw.Grupo
    WHERE
        dw.Grupo IS NULL;

    SET p_inserted_rows = ROW_COUNT();
    
    -- =================================================================================
    -- PASSO 3: LOG DE SUCESSO
    -- =================================================================================
    INSERT INTO HASHTAG_DW.tbInfra_LogCarga (NomeProcedure, Mensagem, LinhasAfetadas) 
    VALUES (
        'spCarga_DimDREFormatos',
        CONCAT('Sucesso! Inseridos: ', IFNULL(p_inserted_rows, 0), ' - Atualizados: ', IFNULL(p_updated_rows, 0)),
        IFNULL(p_inserted_rows, 0) + IFNULL(p_updated_rows, 0)
    );

END //

DELIMITER ;