# core/db_strategies.py

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple, Dict
import pandas as pd
from sqlalchemy import text, select
from sqlalchemy.engine import Connection
import time
import logging

log = logging.getLogger(__name__)

class DbStrategy(ABC):
    """
    Define o contrato (interface) para todas as estratégias de banco de dados.
    Qualquer especialista de banco de dados (MySQL, SQL Server, etc.) deve ser
    capaz de executar estas tarefas fundamentais de ETL.
    """
    @abstractmethod
    def execute_merge(
        self,
        conn: Connection,
        df: pd.DataFrame,
        temp_table_name: str,
        target_table: str,
        key_cols: List[str],
        compare_cols: Optional[List[str]],
        schema: Optional[str]
    ) -> Tuple[int, int]:
        """
        Executa a lógica de MERGE (UPSERT) específica do dialeto.

        Deve retornar uma tupla contendo (linhas_inseridas, linhas_atualizadas).
        Esta contagem deve ser precisa.
        """
        pass

    @abstractmethod
    def execute_procedure(self, conn: Connection, proc_name: str) -> Tuple[int, int]:
        """
        Executa uma Stored Procedure que possui parâmetros de saída para contagem
        de linhas inseridas e atualizadas.

        Deve retornar uma tupla contendo (linhas_inseridas, linhas_atualizadas).
        """
        pass


class MySQLStrategy(DbStrategy):
    """
    Especialista em interações de carga com o MySQL.
    Utiliza a abordagem de UPDATE + INSERT para simular um MERGE.
    """

    def execute_merge(self, conn: Connection, df: pd.DataFrame, temp_table_name: str, target_table: str, key_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> Tuple[int, int]:
        log.debug("Executando estratégia de MERGE para MySQL.")
        # Lógica de UPDATE para registros existentes e modificados
        update_sql = self._build_mysql_update_statement(target_table, temp_table_name, key_cols, list(df.columns), compare_cols, schema)
        result_update = conn.execute(text(update_sql))

        # Lógica de INSERT para novos registros
        insert_sql = self._build_mysql_insert_statement(target_table, temp_table_name, key_cols, list(df.columns), schema)
        result_insert = conn.execute(text(insert_sql))
        
        # O MySQL retorna as contagens de linhas afetadas separadamente para cada comando.
        inserted = result_insert.rowcount if result_insert.rowcount is not None else 0
        updated = result_update.rowcount if result_update.rowcount is not None else 0
        
        return inserted, updated

    def execute_procedure(self, conn: Connection, proc_name: str, params: Dict[str, Any] = None) -> Tuple[int, int]:
        log.debug(f"Executando procedure MySQL: {proc_name}")
        
        # Constrói a lista de parâmetros para a chamada
        param_str = ", ".join([f":{k}" for k in (params or {})])
        
        # A chamada agora inclui os parâmetros, se existirem
        sql_call = f"CALL {proc_name}(@p_inserted_rows, @p_updated_rows, {param_str});"
        
        conn.execute(text(sql_call), (params or {}))
        result = conn.execute(text("SELECT @p_inserted_rows, @p_updated_rows;")).fetchone()
        
        if result and result[0] is not None:
            return int(result[0]), int(result[1])
        
        log.warning(f"Procedure MySQL '{proc_name}' não retornou contagens.")
        return 0, 0

    def _build_mysql_update_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> str:
        target = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        join = " AND ".join([f"dw.`{k}` = tmp.`{k}`" for k in keys])
        
        cols_to_compare = compare_cols if compare_cols is not None else [c for c in all_cols if c not in keys]
        if not cols_to_compare:
            return "SELECT 0; -- Nenhuma coluna para comparar, pulando o UPDATE."
            
        update_set = ", ".join([f"dw.`{c}` = tmp.`{c}`" for c in cols_to_compare])
        # O operador <=> é a forma segura de comparar valores no MySQL, pois trata NULLs corretamente.
        where_diff = " OR ".join([f"NOT (dw.`{c}` <=> tmp.`{c}`)" for c in cols_to_compare])
        
        return f"UPDATE {target} dw JOIN `{temp_table}` tmp ON {join} SET {update_set} WHERE {where_diff};"

    def _build_mysql_insert_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], schema: Optional[str]) -> str:
        target = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        join = " AND ".join([f"dw.`{k}` = tmp.`{k}`" for k in keys])
        cols_to_select = ", ".join([f"tmp.`{c}`" for c in all_cols])
        cols_to_insert = ", ".join([f"`{c}`" for c in all_cols])
        
        return f"""
            INSERT INTO {target} ({cols_to_insert})
            SELECT {cols_to_select} FROM `{temp_table}` tmp
            LEFT JOIN {target} dw ON {join}
            WHERE dw.`{keys[0]}` IS NULL;
        """


class SQLServerStrategy(DbStrategy):
    """
    Especialista em interações de carga com o SQL Server.
    Utiliza o comando MERGE nativo com a cláusula OUTPUT para garantir precisão nos logs.
    """

    def execute_merge(self, conn: Connection, df: pd.DataFrame, temp_table_name: str, target_table: str, key_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> Tuple[int, int]:
        log.debug("Executando estratégia de MERGE para SQL Server.")
        # Tabela temporária global (##) para armazenar o resultado da cláusula OUTPUT.
        # É necessário que seja visível na sessão.
        output_table = f"##output_{int(time.time() * 1000)}"
        
        try:
            # 1. Cria a tabela temporária para capturar os resultados da ação do MERGE.
            conn.execute(text(f"CREATE TABLE {output_table} (acao NVARCHAR(10));"))

            # 2. Constrói e executa o comando MERGE.
            merge_sql = self._build_sqlserver_merge_statement(
                target_table, temp_table_name, key_cols, list(df.columns),
                compare_cols, schema, output_table
            )
            conn.execute(text(merge_sql))

            # 3. Conta os resultados da operação com precisão, consultando a tabela de output.
            inserted_query = select(text("COUNT(*)")).select_from(text(output_table)).where(text("acao = 'INSERT'"))
            inserted = conn.execute(inserted_query).scalar() or 0
            
            updated_query = select(text("COUNT(*)")).select_from(text(output_table)).where(text("acao = 'UPDATE'"))
            updated = conn.execute(updated_query).scalar() or 0
            
            return inserted, updated
        finally:
            # 4. Garante a limpeza da tabela temporária de output em qualquer cenário.
            conn.execute(text(f"DROP TABLE IF EXISTS {output_table};"))

    def execute_procedure(self, conn: Connection, proc_name: str) -> Tuple[int, int]:
        log.debug(f"Executando procedure SQL Server: {proc_name}")
        # SQL Server requer um bloco T-SQL para declarar variáveis, executar a procedure
        # com parâmetros OUTPUT e depois selecionar essas variáveis.
        sql = f"""
            DECLARE @inserted INT, @updated INT;
            EXEC {proc_name} @p_inserted_rows = @inserted OUTPUT, @p_updated_rows = @updated OUTPUT;
            SELECT @inserted, @updated;
        """
        result = conn.execute(text(sql)).fetchone()
        
        if result and result[0] is not None:
            return int(result[0]), int(result[1])

        log.warning(f"Procedure SQL Server '{proc_name}' não retornou contagens.")
        return 0, 0

    def _build_sqlserver_merge_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str], output_table: str) -> str:
        target = f"[{schema}].[{table}]" if schema else f"[{table}]"
        join = " AND ".join([f"target.[{k}] = source.[{k}]" for k in keys])
        
        cols_to_compare = compare_cols if compare_cols is not None else [c for c in all_cols if c not in keys]
        
        update_clause = ""
        # A cláusula de UPDATE só é adicionada se houver colunas para comparar.
        if cols_to_compare:
            update_set = ", ".join([f"target.[{c}] = source.[{c}]" for c in cols_to_compare])
            # Compara os valores convertendo para string para uma verificação genérica e segura de NULLs.
            # Para alta performance em tabelas muito grandes, uma comparação tipo a tipo seria melhor,
            # mas esta é uma abordagem geral robusta.
            where_diff = " OR ".join([f"ISNULL(CONVERT(NVARCHAR(MAX), target.[{c}]), '') <> ISNULL(CONVERT(NVARCHAR(MAX), source.[{c}]), '')" for c in cols_to_compare])
            update_clause = f"""
            WHEN MATCHED AND ({where_diff}) THEN
                UPDATE SET {update_set}
            """
        
        cols_insert = ", ".join([f"[{c}]" for c in all_cols])
        cols_values = ", ".join([f"source.[{c}]" for c in all_cols])

        return f"""
            MERGE INTO {target} AS target
            USING {temp_table} AS source ON ({join})
            {update_clause}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({cols_insert}) VALUES ({cols_values})
            OUTPUT $action INTO {output_table};
        """