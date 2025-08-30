-- =================================================================================
-- TEMPLATE PARA PROCEDURE DE CARGA INCREMENTAL (UPSERT) COM MERGE - SQL SERVER
--
-- INSTRUÇÕES DE USO:
-- 1. Copie este arquivo e renomeie para "spCarga_Dim<SuaTabela>.sql".
-- 2. Use a função "Localizar e Substituir" do seu editor para trocar os
--    placeholders abaixo pelos valores corretos.
--
-- PLACEHOLDERS:
-- __NOME_PROCEDURE__     -> Ex: spCarga_DimCliente
-- __SCHEMA_DW__          -> Ex: dbo
-- __TABELA_DW__          -> Ex: tbDim_Cliente
-- __SCHEMA_STG__         -> Ex: stg
-- __TABELA_STG__         -> Ex: tbSTG_Cliente
-- __CHAVE_NEGOCIO__      -> Coluna para o JOIN. Ex: ClienteID
-- __COLUNAS_UPDATE__     -> Lista de colunas para o SET. Ex: target.Nome = source.Nome, target.Cidade = source.Cidade
-- __COLUNAS_INSERT__     -> Lista de colunas para o INSERT. Ex: ClienteID, Nome, Cidade
-- __VALORES_INSERT__     -> Lista de valores do VALUES. Ex: source.ClienteID, source.Nome, source.Cidade
-- __COLUNAS_WHERE__      -> Condições para verificar se houve alteração.
--                           Ex: target.Nome <> source.Nome OR target.Cidade <> source.Cidade
-- =================================================================================

-- Usamos CREATE OR ALTER para que a procedure seja criada se não existir, ou alterada se já existir.
-- É mais prático do que o bloco DROP IF EXISTS + CREATE.
CREATE OR ALTER PROCEDURE __NOME_PROCEDURE__ (
    @p_inserted_rows INT OUTPUT,
    @p_updated_rows INT OUTPUT
)
AS
BEGIN
    -- No SQL Server, SET NOCOUNT ON impede que o SQL Server envie mensagens de contagem de linhas
    -- para o cliente para cada instrução, melhorando a performance.
    SET NOCOUNT ON;

    -- =================================================================================
    -- DECLARAÇÕES E CONFIGURAÇÃO INICIAL
    -- =================================================================================
    DECLARE @nome_procedure VARCHAR(128) = '__NOME_PROCEDURE__';
    DECLARE @mensagem_log TEXT;
    DECLARE @linhas_afetadas INT = 0;

    -- Tabela temporária para capturar os resultados da operação MERGE.
    -- O comando OUTPUT do MERGE nos permite saber exatamente o que foi feito em cada linha (INSERT, UPDATE ou DELETE).
    DECLARE @MergeOutput TABLE (
        acao NVARCHAR(10)
    );

    -- =================================================================================
    -- PASSO 1: INICIAR TRANSAÇÃO E TRATAMENTO DE ERRO
    -- O bloco TRY...CATCH é a forma padrão de tratamento de erros no SQL Server.
    -- =================================================================================
    BEGIN TRY
        -- Inicia a transação para garantir a atomicidade. Ou tudo funciona, ou nada é salvo.
        BEGIN TRANSACTION;

        -- =================================================================================
        -- PASSO 2: EXECUTAR A OPERAÇÃO DE MERGE (UPSERT)
        -- O MERGE consolida as operações de UPDATE e INSERT em um único comando atômico.
        -- =================================================================================
        MERGE INTO __SCHEMA_DW__.__TABELA_DW__ AS target
        USING __SCHEMA_STG__.__TABELA_STG__ AS source
            ON (target.__CHAVE_NEGOCIO__ = source.__CHAVE_NEGOCIO__)

        -- AÇÃO 1: WHEN MATCHED -> Registros que existem em ambas as tabelas (target e source).
        -- O 'AND' adicional garante que o UPDATE só ocorra se houver de fato uma mudança.
        WHEN MATCHED AND (__COLUNAS_WHERE__) THEN
            UPDATE SET
                __COLUNAS_UPDATE__

        -- AÇÃO 2: WHEN NOT MATCHED BY TARGET -> Registros que existem na source, mas não no target (novos registros).
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (__COLUNAS_INSERT__)
            VALUES (__VALORES_INSERT__)

        -- A cláusula OUTPUT é poderosa. Ela captura a ação ($action) para cada linha afetada
        -- e insere em nossa tabela temporária @MergeOutput.
        OUTPUT $action INTO @MergeOutput;

        -- =================================================================================
        -- PASSO 3: CAPTURAR RESULTADOS E LOGAR SUCESSO
        -- =================================================================================

        -- Contamos as ações que capturamos na tabela @MergeOutput.
        SET @p_inserted_rows = (SELECT COUNT(*) FROM @MergeOutput WHERE acao = 'INSERT');
        SET @p_updated_rows = (SELECT COUNT(*) FROM @MergeOutput WHERE acao = 'UPDATE');
        SET @linhas_afetadas = ISNULL(@p_inserted_rows, 0) + ISNULL(@p_updated_rows, 0);

        SET @mensagem_log = CONCAT('Sucesso! Inseridos: ', @p_inserted_rows, ' - Atualizados: ', @p_updated_rows);
        INSERT INTO __SCHEMA_DW__.tbInfra_LogCarga (NomeProcedure, Mensagem, LinhasAfetadas)
        VALUES (@nome_procedure, @mensagem_log, @linhas_afetadas);

        -- Se tudo correu bem, confirma a transação.
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- =================================================================================
        -- PASSO 4: TRATAMENTO DE ERRO (BLOCO CATCH)
        -- =================================================================================
        -- Se a transação ainda estiver ativa, desfaz todas as alterações.
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Captura a mensagem de erro original do SQL Server.
        SET @mensagem_log = ERROR_MESSAGE();

        -- Grava o erro na tabela de log.
        INSERT INTO __SCHEMA_DW__.tbInfra_LogCarga (NomeProcedure, Mensagem)
        VALUES (@nome_procedure, CONCAT('ERRO: ', @mensagem_log));

        -- Opcional: Lança o erro para a aplicação que chamou a procedure.
        -- Isso é útil para que o orquestrador (ex: Python) saiba que a execução falhou.
        THROW;

    END CATCH;

    -- Garante que a contagem de linhas volte ao comportamento padrão ao final da procedure.
    SET NOCOUNT OFF;
END;
GO -- O GO finaliza o "batch" de comandos para o SQL Server.