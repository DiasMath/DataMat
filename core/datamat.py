from __future__ import annotations
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandera as pa
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError

from core.errors.exceptions import (
    DataMatError,
    DataExtractionError,
    DataValidationError,
    DataLoadError
)

log = logging.getLogger(__name__)


@dataclass
class DataMatConfig:
    """Define o contrato de configuração para a classe DataMat."""
    ingest_if_exists: str
    ingest_chunksize: int
    ingest_method: Optional[str]
    etl_log_table: str


class DataMat:
    """
    Biblioteca de ferramentas de ETL, responsável por toda a interação com o
    banco de dados e pela execução das etapas de um pipeline.
    """

    def __init__(self, engine: Engine, config: DataMatConfig) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("O parâmetro 'engine' deve ser uma instância de sqlalchemy.engine.Engine.")
        if not isinstance(config, DataMatConfig):
            raise TypeError("O parâmetro 'config' deve ser uma instância de DataMatConfig.")

        self.engine = engine
        self.config = config
        self.log = logging.getLogger(f"DataMat.{engine.url.database}")

    def run_etl_job(self, adapter: Any, job_config: Any, mapping_spec: Any) -> Tuple[str, int, int]:
        job_name = job_config.name
        self.log.info(f"▶️  [{job_name}] Iniciando job...")
        t_start = time.perf_counter()
        try:
            df = self._extract(adapter, job_name)
            df = self._prepare_and_map(df, job_config, mapping_spec, job_name)
            eff_keys = self._get_effective_keys(job_config, mapping_spec)
            df = self._deduplicate(df, eff_keys, job_name)
            self._validate(df, eff_keys, mapping_spec, job_name)
            df = self._transform(df, job_name)
            inserted, updated = self._load(df, job_config, eff_keys, mapping_spec, job_name)
            self.log.info(f"🎉 [{job_name}] Carga concluída: {inserted} inseridos, {updated} atualizados.")
            self.log.info(f"⏱️  [{job_name}] Tempo total do job: {time.perf_counter() - t_start:.2f}s")
            return job_name, inserted, updated
        except DataMatError:
            raise
        except Exception as e:
            self.log.error(f"❌ [{job_name}] Erro não esperado no job: {e}", exc_info=False)
            raise DataMatError(f"Job '{job_name}' falhou devido a um erro inesperado.") from e

    def run_dw_procedure(self, proc_name: str, resilient: bool = True) -> Tuple[int, int]:
        self.log.info(f"   -> Executando procedure: {proc_name}...")
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"CALL {proc_name}(@p_inserted_rows, @p_updated_rows);"))
                    result = conn.execute(text("SELECT @p_inserted_rows, @p_updated_rows;")).fetchone()
                if result and result[0] is not None:
                    inserted, updated = int(result[0]), int(result[1])
                    self.log.info(f"   -> ✅ {proc_name}: {inserted} inseridos, {updated} atualizadas.")
                    return inserted, updated
                else:
                    self.log.warning(f"   -> ⚠️  {proc_name}: Executada, mas não retornou contagens.")
                    return 0, 0
        except SQLAlchemyError as e:
            self.log.error(f"   -> ❌ FALHA na procedure: {proc_name} - {e}", exc_info=True)
            self.log_etl_error(process_name=proc_name, message=str(e))
            if not resilient:
                raise DataLoadError(f"Falha ao executar a procedure '{proc_name}'.") from e
            return 0, 0

    def synchronize_dw_objects(self, dw_base_path: Path, objects_to_sync: List[List[Dict]]) -> int:
        self.log.info("🔄 Sincronizando objetos do DW...")
        if not objects_to_sync:
            self.log.info("Nenhum objeto do DW definido para sincronização.")
            return 0
        synced_count = 0
        for i, group in enumerate(objects_to_sync):
            self.log.info(f"   -> Executando grupo de sincronização {i+1}...")
            for object_info in group:
                file_path = dw_base_path / object_info["file"]
                self.log.info(f"      -> Aplicando {object_info['file']}...")
                if not file_path.exists():
                    self.log.error(f"         -> ❌ ARQUIVO NÃO ENCONTRADO: {file_path}")
                    continue
                try:
                    script_content = file_path.read_text(encoding="utf-8")
                    self._execute_sql_script(script_content)
                    synced_count += 1
                except Exception as e:
                    self.log.error(f"         -> ❌ FALHA ao aplicar script {file_path.name}: {e}", exc_info=True)
        self.log.info(f"✅ {synced_count} objetos do DW sincronizados.")
        return synced_count

    def log_etl_error(self, process_name: str, message: str) -> None:
        try:
            error_message = f"ERRO: {message[:65000]}"
            sql = text(f"INSERT INTO {self.config.etl_log_table} (NomeProcedure, Mensagem, LinhasAfetadas) VALUES (:name, :msg, 0)")
            with self.engine.begin() as conn:
                conn.execute(sql, {"name": process_name, "msg": error_message})
        except Exception as e:
            self.log.error(f"FALHA CRÍTICA: Não foi possível registrar o erro no banco. Causa: {e}")

    @staticmethod
    def log_summary(client_id: str, stg_results: List[Tuple[str, int, int]], proc_results: List[Tuple[int, int]]) -> None:
        stg_inserted = sum(i for _, i, _ in stg_results if i != -1)
        stg_updated = sum(u for _, _, u in stg_results if u != -1)
        proc_inserted = sum(i for i, _ in proc_results)
        proc_updated = sum(u for _, u in proc_results)
        log.info("\n" + "="*50)
        log.info(f"📊 RESUMO FINAL DA CARGA PARA O CLIENTE: {client_id}")
        log.info("="*50)
        log.info(f"STG    - Total Inserido:   {stg_inserted}")
        log.info(f"STG    - Total Atualizado: {stg_updated}")
        log.info(f"PROCS  - Total Inserido:   {proc_inserted}")
        log.info(f"PROCS  - Total Atualizado: {proc_updated}")
        log.info("="*50 + "\n")

    def _extract(self, adapter: Any, job_name: str) -> pd.DataFrame:
        self.log.info(f"[{job_name}] Extraindo dados...")
        t0 = time.perf_counter()
        try:
            df = adapter.extract()
            self.log.info(f"✅ [{job_name}] Extração concluída: {len(df)} linhas em {time.perf_counter()-t0:.2f}s")
            return df
        except Exception as e:
            raise DataExtractionError(f"Falha na extração para o job '{job_name}'.") from e

    def _prepare_and_map(self, df: pd.DataFrame, job_config: Any, mapping_spec: Any, job_name: str) -> pd.DataFrame:
        if df.empty: 
            return df
        self.log.info(f"🔧 [{job_name}] Preparando e mapeando dataframe...")
        if not mapping_spec:
             raise DataMatError(f"map_id '{getattr(job_config, 'map_id', 'N/A')}' não foi encontrado no mappings.py")
        w = df.copy()
        if src_cols := list(mapping_spec.src_to_tgt.keys()):
            if missing := [c for c in src_cols if c not in w.columns]:
                raise DataMatError(f"Colunas de origem ausentes: {missing}")
            w = w[src_cols]
        if rename_map := mapping_spec.src_to_tgt:
            w = w.rename(columns=rename_map)
        for c in w.select_dtypes(include=['object', 'string']).columns:
            w[c] = w[c].str.strip()
        return w

    def _deduplicate(self, df: pd.DataFrame, keys: List[str], job_name: str) -> pd.DataFrame:
        if df.empty or not keys: 
            return df
        self.log.info(f"🧹 [{job_name}] Removendo duplicatas pela chave {keys}...")
        before = len(df)
        df_dedup = df.drop_duplicates(subset=keys, keep='last').reset_index(drop=True)
        if before > len(df_dedup):
            self.log.info(f"✅ [{job_name}] Dedup: {before} -> {len(df_dedup)} linhas.")
        return df_dedup

    def _validate(self, df: pd.DataFrame, keys: List[str], mapping_spec: Any, job_name: str) -> None:
        if df.empty: 
            return
        self.log.info(f"🔎 [{job_name}] Validando qualidade dos dados...")
        validation_rules = getattr(mapping_spec, 'validation_rules', {})
        if not validation_rules and not keys: 
            return
        try:
            schema_cols = {
                col: rules if isinstance(rules, pa.Column) else pa.Column(**rules)
                for col, rules in validation_rules.items() 
                }
            for key in keys:
                schema_cols[key] = schema_cols.get(key, pa.Column())
                schema_cols[key].properties.update({'unique': True, 'required': True, 'nullable': False})
            schema = pa.DataFrameSchema(columns=schema_cols, strict=False, coerce=True)
            schema.validate(df, lazy=True)
            self.log.info(f"✅ [{job_name}] Validação de dados concluída.")
        except pa.errors.SchemaErrors as err:
            message = f"Falha na validação de dados:\n{err.failure_cases.to_markdown(index=False)}"
            raise DataValidationError(message) from err

    def _transform(self, df: pd.DataFrame, job_name: str) -> pd.DataFrame:
        if df.empty: 
            return df
        self.log.info(f"💰 [{job_name}] Aplicando transformações genéricas...")
        def _normalize_money(s: pd.Series) -> pd.Series:
            if pd.api.types.is_numeric_dtype(s): 
                return s.round(4)
            if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
                txt = s.astype(str).str.replace(r'[^\d,\.-]', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                return pd.to_numeric(txt, errors="coerce").round(4)
            return s
        money_cols = [c for c in df.columns if any(tok in c.lower() for tok in ("valor", "preco", "preço", "custo", "total"))]
        for col in money_cols: 
            df[col] = _normalize_money(df[col])
        return df

    def _load(self, df: pd.DataFrame, job_config: Any, keys: List[str], mapping_spec: Any, job_name: str) -> Tuple[int, int]:
        if df.empty: 
            return 0, 0
        table = job_config.table
        schema = os.getenv(job_config.db_name) 
        if not schema:
            raise ValueError(f"A variável de ambiente para o banco de dados '{job_config.db_name}' não está definida.")
        self.log.info(f"🚚 [{job_name}] Carregando {len(df)} linhas para '{schema}.{table}'...")
        try:
            if keys and self.engine.dialect.name.lower() == "mysql":
                compare_cols = getattr(mapping_spec, "compare_cols", None)
                return self._merge_into_mysql(df, table, keys, compare_cols, schema, job_name)
            else:
                return self._append_to_db(df, table, schema)
        except Exception as e:
            raise DataLoadError(f"Falha na carga para a tabela '{table}'.") from e

    def _append_to_db(self, df: pd.DataFrame, table_name: str, schema: Optional[str]) -> Tuple[int, int]:
        df.to_sql(
            table_name, con=self.engine, schema=schema, if_exists=self.config.ingest_if_exists,
            index=False, chunksize=self.config.ingest_chunksize, method=self.config.ingest_method
        )
        return len(df), 0

    def _merge_into_mysql(self, df: pd.DataFrame, table_name: str, key_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str], job_name: str) -> Tuple[int, int]:
        temp_table_name = f"temp_{job_name.lower().replace(' ', '_')}_{int(time.time())}"
        with self.engine.connect() as conn:
            try:
                with conn.begin() as transaction:
                    df_coerced = self._coerce_df_types_from_db_schema(df, table_name, schema, conn, job_name)
                    df_coerced.to_sql(temp_table_name, conn, if_exists='replace', index=False)
                    
                    update_sql = self._build_mysql_update_statement(table_name, temp_table_name, key_cols, df.columns, compare_cols, schema)
                    result_update = conn.execute(text(update_sql))
                    
                    insert_sql = self._build_mysql_insert_statement(table_name, temp_table_name, key_cols, df.columns, schema)
                    result_insert = conn.execute(text(insert_sql))
                    
                    return result_insert.rowcount, result_update.rowcount
            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS `{temp_table_name}`;"))

    def _execute_sql_script(self, script_content: str) -> None:
        script_content = re.sub(r'--.*?\n', '', script_content)
        script_content = re.sub(r'/\*.*?\*/', '', script_content, flags=re.DOTALL)
        statements = re.split(r'DELIMITER\s+([^\s]+)', script_content, flags=re.IGNORECASE)
        current_delimiter = ';'
        with self.engine.connect() as conn, conn.begin():
            commands = [cmd.strip() for cmd in statements[0].split(current_delimiter) if cmd.strip()]
            for cmd in commands:
                conn.execute(text(cmd))
            for i in range(1, len(statements), 2):
                current_delimiter = statements[i]
                sql_block = statements[i+1]
                commands = [cmd.strip() for cmd in sql_block.split(current_delimiter) if cmd.strip()]
                for cmd in commands:
                    conn.execute(text(cmd))

    def _build_mysql_update_statement(self, table: str, temp_table: str, keys: List[str], all_cols: List[str], compare_cols: Optional[List[str]], schema: Optional[str]) -> str:
        target = f"`{schema}`.`{table}`" if schema else f"`{table}`"
        join = " AND ".join([f"dw.`{k}` = tmp.`{k}`" for k in keys])
        cols_to_compare = compare_cols if compare_cols is not None else [c for c in all_cols if c not in keys]
        if not cols_to_compare:
            return "SELECT 0; -- No columns to compare, skipping update"
        update_set = ", ".join([f"dw.`{c}` = tmp.`{c}`" for c in cols_to_compare])
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

    def _coerce_df_types_from_db_schema(self, df: pd.DataFrame, table_name: str, schema: Optional[str], conn: Connection, job_name: str) -> pd.DataFrame:
        df_coerced = df.copy()
        try:
            inspector = inspect(conn)
            columns_info = {c['name']: str(c['type']).lower() for c in inspector.get_columns(table_name, schema=schema)}
            for col, col_type in columns_info.items():
                if col in df_coerced.columns:
                    series = df_coerced[col]
                    if 'int' in col_type:
                        df_coerced[col] = pd.to_numeric(series, errors='coerce').astype('Int64')
                    elif 'datetime' in col_type or 'timestamp' in col_type:
                        df_coerced[col] = pd.to_datetime(series, errors='coerce')
                    elif any(t in col_type for t in ["decimal", "float", "double", "numeric"]):
                        df_coerced[col] = pd.to_numeric(series, errors='coerce')
                    elif 'char' in col_type or 'text' in col_type:
                        df_coerced[col] = series.astype(str).replace('nan', '')
        except Exception as e:
            self.log.warning(f"[{job_name}] Não foi possível inspecionar a tabela '{table_name}'. Prosseguindo com tipos inferidos. Erro: {e}")
        return df_coerced

    def _get_effective_keys(self, job_config: Any, mapping_spec: Any) -> List[str]:
        keys = getattr(mapping_spec, "key_cols", [])
        return [keys] if isinstance(keys, str) else keys