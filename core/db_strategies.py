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
        schema: Optional[str],
        **kwargs
    ) -> Tuple[int, int]:
        """
        Executa a lógica de MERGE (UPSERT) específica do dialeto.
        Deve retornar uma tupla contendo (linhas_inseridas, linhas_atualizadas).
        """
        pass

    @abstractmethod
    def execute_procedure(self, conn: Connection, proc_name: str) -> Tuple[int, int]:
        """
        Executa uma Stored Procedure que possui parâmetros de saída para contagem
        de linhas inseridas e atualizadas.
        """
        pass


class MySQLStrategy(DbStrategy):
    """
    Especialista em interações de carga com o MySQL.
    Suporta dois modos:
    1. 'legacy' (Padrão): UPDATE + INSERT (Seguro para tabelas SEM Unique Key)
    2. 'iodku': INSERT ON DUPLICATE KEY UPDATE (Alta performance, exige Unique Key)
    """

    def execute_merge(
        self, 
        conn: Connection, 
        df: pd.DataFrame, 
        temp_table_name: str, 
        target_table: str, 
        key_cols: List[str], 
        compare_cols: Optional[List[str]], 
        schema: Optional[str],
        **kwargs
    ) -> Tuple[int, int]:
        
        # Verifica qual modo de merge foi solicitado. Padrão é 'legacy'.
        merge_mode = kwargs.get('merge_mode', 'legacy')

        if merge_mode == 'iodku':
            return self._execute_iodku_merge(conn, df, temp_table_name, target_table, key_cols, compare_cols, schema)
        else:
            return self._execute_legacy_merge(conn, df, temp_table_name, target_table, key_cols, compare_cols, schema)

    # =========================================================================
    #  INSERT ON DUPLICATE KEY UPDATE (IODKU)
    #  Recomendado para tabelas com PK ou Unique Index definidos.
    # =========================================================================
    def _execute_iodku_merge(self, conn: Connection, df: pd.DataFrame, temp_table_name: str, target_table: str, key_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> Tuple[int, int]:
        target = f"`{schema}`.`{target_table}`" if schema else f"`{target_table}`"
        all_cols = list(df.columns)
        # Prepara a lista de colunas para o INSERT: `col1`, `col2`...
        cols_str = ", ".join([f"`{c}`" for c in all_cols])
        
        # Define quais colunas atualizar se a chave duplicar.
        # Se compare_cols for None, atualiza tudo exceto as chaves (comportamento padrão de upsert)
        cols_to_update = compare_cols if compare_cols is not None else [c for c in all_cols if c not in key_cols]
        
        if not cols_to_update:
            # Caso especial: Se não tem nada para atualizar (ex: tabela só de IDs),
            # usamos INSERT IGNORE para apenas ignorar duplicatas sem erro.
            log.debug(f"[{target_table}] IODKU: Nada a atualizar. Usando INSERT IGNORE.")
            sql = f"INSERT IGNORE INTO {target} ({cols_str}) SELECT {cols_str} FROM `{temp_table_name}`;"
            result = conn.execute(text(sql))
            return result.rowcount, 0

        # Monta a cláusula ON DUPLICATE KEY UPDATE
        # Sintaxe: col = VALUES(col) -> Pega o valor que veio da tabela temporária
        update_assignments = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in cols_to_update])
        
        sql = f"""
            INSERT INTO {target} ({cols_str})
            SELECT {cols_str} FROM `{temp_table_name}`
            ON DUPLICATE KEY UPDATE {update_assignments};
        """
        
        log.debug(f"[{target_table}] IODKU SQL:\n{sql}")
        log.debug(f"[{target_table}] Executando IODKU (Merge Atômico)...")
        result = conn.execute(text(sql))
        
        # --- LÓGICA DE CÁLCULO DE LINHAS DO MYSQL ---
        # O MySQL retorna rowcount com a seguinte lógica no IODKU:
        # 1 = Inserção de novo registro
        # 2 = Atualização de registro existente
        # 0 = Registro existe e é IDÊNTICO (sem mudança)
        
        total_rows_input = len(df)
        rc = result.rowcount
        
        if rc > 0:
            if rc < total_rows_input:
                # Se rowcount é menor que o total de entrada, significa que houve muitos '0' (sem mudança).
                # Como é difícil saber exato, assumimos atualização para simplificar ou zero inserções.
                inserted = 0
                updated = rc 
            else:
                # Matemática aproximada:
                # updates = rc - total_entrada
                updated_calc = rc - total_rows_input
                updated = max(0, updated_calc)
                inserted = max(0, total_rows_input - updated)
        else:
            inserted, updated = 0, 0
            
        return inserted, updated

    # =========================================================================
    #  MODO LEGADO: UPDATE + INSERT
    #  Seguro para tabelas sem Unique Key, mas não é atômico.
    # =========================================================================
    def _execute_legacy_merge(self, conn: Connection, df: pd.DataFrame, temp_table_name: str, target_table: str, key_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> Tuple[int, int]:
        log.debug(f"[{target_table}] Executando MERGE Legado (Update + Insert)...")
        
        # 1. Executa UPDATE
        update_sql = self._build_mysql_update_statement(target_table, temp_table_name, key_cols, list(df.columns), compare_cols, schema)
        
        log.debug(f"[{target_table}] LEGACY UPDATE SQL:\n{update_sql}")

        result_update = conn.execute(text(update_sql))

        # 2. Executa INSERT (dos novos)
        insert_sql = self._build_mysql_insert_statement(target_table, temp_table_name, key_cols, list(df.columns), schema)

        log.debug(f"[{target_table}] LEGACY UPDATE SQL:\n{update_sql}")

        result_insert = conn.execute(text(insert_sql))
        
        # Coleta estatísticas
        inserted = result_insert.rowcount if result_insert.rowcount is not None else 0
        updated = result_update.rowcount if result_update.rowcount is not None else 0
        
        return inserted, updated

    def execute_procedure(self, conn: Connection, proc_config: Dict[str, Any]) -> Tuple[int, int]:
        proc_name = proc_config["sql"]
        # Pega a flag da configuração. O padrão é True se a chave não existir.
        use_out_params = proc_config.get("use_out_params", True)

        log.debug(f"Executando procedure MySQL: {proc_name} (use_out_params={use_out_params})")
        
        # 1. Monta a lista de parâmetros de entrada (IN), se houver.
        params = proc_config.get("params", {})
        in_params_list = [f":{k}" for k in params.keys()] if params else []
        
        all_params_list = in_params_list
        
        # 2. SÓ adiciona os parâmetros de SAÍDA (OUT) se a flag for True.
        if use_out_params:
            out_params_list = ['@p_inserted_rows', '@p_updated_rows']
            # Junta as duas listas, garantindo que os de saída venham primeiro se for o padrão
            all_params_list = out_params_list + in_params_list
            
        # 3. Constrói a string de parâmetros de forma segura.
        params_str = ", ".join(all_params_list)
        
        # 4. Monta a chamada SQL final.
        sql_call = f"CALL {proc_name}({params_str});"
        
        try:
            conn.execute(text(sql_call), (params or {}))
            
            # 5. SÓ tenta buscar o resultado se os parâmetros de saída foram usados.
            if use_out_params:
                result = conn.execute(text("SELECT @p_inserted_rows, @p_updated_rows;")).fetchone()
                if result and result[0] is not None:
                    return int(result[0]), int(result[1])
                log.warning(f"Procedure MySQL '{proc_name}' deveria retornar contagens, mas não o fez.")
            
            return 0, 0
        except Exception as e:
            log.error(f"Erro ao executar a procedure '{proc_name}'. SQL gerado: {sql_call}")
            raise e
    
    # --- MÉTODOS AUXILIARES LEGADOS (MANTIDOS COM AJUSTE DE SEGURANÇA) ---
    def _build_mysql_update_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> str:
        target = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        
        # JORGE: O operador <=> (spaceship) é crucial aqui. Ele compara NULLs corretamente (NULL <=> NULL é True).
        # Sem isso, chaves nulas duplicam registros infinitamente.
        join = " AND ".join([f"dw.`{k}` <=> tmp.`{k}`" for k in keys])
        
        cols_to_compare = compare_cols if compare_cols is not None else [c for c in all_cols if c not in keys]
        if not cols_to_compare:
            return "SELECT 0; -- Nada a atualizar"
            
        update_set = ", ".join([f"dw.`{c}` = tmp.`{c}`" for c in cols_to_compare])
        where_diff = " OR ".join([f"NOT (dw.`{c}` <=> tmp.`{c}`)" for c in cols_to_compare])
        
        return f"UPDATE {target} dw JOIN `{temp_table}` tmp ON {join} SET {update_set} WHERE {where_diff};"

    def _build_mysql_insert_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], schema: Optional[str]) -> str:
        target = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        
        # JORGE: Mesmo no insert, usamos <=> para garantir que o LEFT JOIN encontre o registro se ele existir (mesmo com chave nula)
        join = " AND ".join([f"dw.`{k}` <=> tmp.`{k}`" for k in keys])
        
        cols_to_select = ", ".join([f"tmp.`{c}`" for c in all_cols])
        cols_to_insert = ", ".join([f"`{c}`" for c in all_cols])
        
        return f"""
            INSERT INTO {target} ({cols_to_insert})
            SELECT {cols_to_select} FROM `{temp_table}` tmp
            LEFT JOIN {target} dw ON {join}
            WHERE dw.`{keys[0]}` IS NULL;
        """
