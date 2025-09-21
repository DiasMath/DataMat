from __future__ import annotations
from datetime import datetime, timedelta
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError

from core.errors.exceptions import (
    DataMatError,
    DataExtractionError,
    DataValidationError,
    DataLoadError
)
# Importe os especialistas (Strategies) e o mapa de seleção
from core.db_strategies import DbStrategy, MySQLStrategy, SQLServerStrategy

log = logging.getLogger(__name__)

# O mapa de estratégias atua como uma fábrica.
# Ele mapeia o nome do dialeto do SQLAlchemy para a classe de estratégia correta.
STRATEGY_MAP = {
    "mysql": MySQLStrategy(),
    "mssql": SQLServerStrategy(),
}


@dataclass
class DataMatConfig:
    """Define o contrato de configuração para a classe DataMat."""
    ingest_if_exists: str
    ingest_chunksize: int
    ingest_method: Optional[str]
    etl_log_table: str


class DataMat:
    """
    Orquestrador de ETL. Gerencia o fluxo do pipeline (E-T-L) e delega
    as operações específicas de banco de dados para a estratégia apropriada.
    """

    def __init__(self, engine: Engine, config: DataMatConfig, preview_limit: int = 0) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("O parâmetro 'engine' deve ser uma instância de sqlalchemy.engine.Engine.")
        if not isinstance(config, DataMatConfig):
            raise TypeError("O parâmetro 'config' deve ser uma instância de DataMatConfig.")

        self.engine = engine
        self.config = config
        self.dialect = engine.dialect.name.lower()
        self.preview_limit = preview_limit
        self.log = logging.getLogger(f"DataMat.{engine.url.database or 'server'}")

        # Seleciona o "especialista" (estratégia) correto com base no dialeto do banco de dados.
        self.strategy: DbStrategy = STRATEGY_MAP.get(self.dialect)
        if not self.strategy:
            self.log.error(f"Nenhuma estratégia de banco de dados encontrada para o dialeto '{self.dialect}'.")
            raise NotImplementedError(f"O dialeto '{self.dialect}' não tem uma estratégia de carga implementada.")
        
        self.log.info(f"DataMat inicializado com a estratégia '{self.strategy.__class__.__name__}'.")

    def run_etl_job(self, adapter: Any, job_config: Any, mapping_spec: Any) -> Tuple[str, int, int]:
        """Executa o ciclo de vida completo de um job de ETL: E -> T -> L."""
        job_name = job_config.name
        self.log.info(f"▶️  [{job_name}] Iniciando job...")
        t_start = time.perf_counter()
        try:
            df = self._extract_and_transform(adapter, job_config, mapping_spec, job_name)
            
            inserted, updated = self._load(df, job_config, mapping_spec, job_name)

            self.log.info(f"🎉 [{job_name}] Carga concluída: {inserted} inseridos, {updated} atualizados.")
            self.log.info(f"⏱️  [{job_name}] Tempo total do job: {time.perf_counter() - t_start:.2f}s")
            return job_name, inserted, updated
        except DataMatError:
            raise
        except Exception as e:
            self.log.error(f"❌ [{job_name}] Erro não esperado no job: {e}", exc_info=False)
            raise DataMatError(f"Job '{job_name}' falhou devido a um erro inesperado.") from e

    def run_etl_job_extract_only(self, adapter: Any, job_config: Any, mapping_spec: Any) -> pd.DataFrame:
        """Executa apenas as fases de extração e transformação, para preview ou export."""
        job_name = job_config.name
        self.log.info(f"▶️  [{job_name}] Iniciando job em modo 'extract-only'...")
        return self._extract_and_transform(adapter, job_config, mapping_spec, job_name)

    def run_dw_procedure(self, proc_config: Dict, resilient: bool = True) -> Tuple[int, int]:
        proc_name = proc_config["name"]
        self.log.info(f"   -> Delegando execução da procedure '{proc_name}' para a estratégia.")
        
        try:
            inc_config = proc_config.get('incremental_config', None)
            params = {}
            if inc_config and inc_config.get("enabled", False):
                days = inc_config.get("days_to_load", 30)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                # Prepara os parâmetros para a chamada da procedure
                params = {
                    "p_data_inicio": start_date.strftime("%Y-%m-%d"),
                    "p_data_fim": end_date.strftime("%Y-%m-%d")
                }
                log.info(f"   -> Carga incremental ativada para '{proc_name}'. Carregando {days} dias.")

            with self.engine.connect() as conn:
                with conn.begin():
                    # A estratégia agora recebe os parâmetros dinâmicos
                    inserted, updated = self.strategy.execute_procedure(conn, proc_name, params)
            
            self.log.info(f"   -> ✅ {proc_name}: {inserted} inseridos, {updated} atualizadas.")
            return inserted, updated
        except SQLAlchemyError as e:
            self.log.error(f"   -> ❌ FALHA na procedure: {proc_name} - {e}", exc_info=True)
            self.log_etl_error(process_name=proc_name, message=str(e))
            if not resilient:
                raise DataLoadError(f"Falha ao executar a procedure '{proc_name}'.") from e
            return 0, 0
        
    def export_job_to_excel(self, adapter: Any, job_config: Any, mapping_spec: Any, tenant_id: str, root_dir: Path, limit: int) -> None:
        """
        Executa a extração de um job e exporta o resultado para um arquivo Excel
        na pasta de dados do tenant.
        """
        job_name = job_config.name
        self.log.info(f"Executando em modo EXPORT para o job '{job_name}'")
        
        # 1. Extrai e transforma os dados usando o método existente
        df = self.run_etl_job_extract_only(adapter, job_config, mapping_spec)
        
        # 2. Imprime o preview no console
        print(f"\n--- Preview do Job: {job_name} ---")
        print(df.head(limit))
        print(f"Total de linhas extraídas: {len(df)}")
        print(f"Tipos de dados:\n{df.dtypes}")
        
        # 3. Delega a lógica de salvar o arquivo para um método privado
        self._save_df_to_excel(df, tenant_id, job_name, root_dir)
    
    # --- MÉTODOS DE LOG E UTILITÁRIOS (INTACTOS) ---

    def log_etl_error(self, process_name: str, message: str) -> None:
        try:
            error_message = f"ERRO: {message[:65000]}"
            table_name = self.config.etl_log_table
            db_name = os.getenv("DB_DW_NAME") # Log de erros geralmente fica no DW
            target = f"{db_name}.{table_name}" if db_name else table_name

            sql = text(f"INSERT INTO {target} (NomeProcedure, Mensagem, LinhasAfetadas) VALUES (:name, :msg, 0)")
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

    # --- MÉTODOS PRIVADOS DO FLUXO DE ETL ---
    
    def _extract_and_transform(self, adapter: Any, job_config: Any, mapping_spec: Any, job_name: str) -> pd.DataFrame:
        """Agrupa as etapas de extração e transformação."""
        # 1. Extrai os dados brutos (ainda no formato JSON/Dict aninhado)
        raw_data = self._extract(adapter, job_name)
        if not raw_data:
            return pd.DataFrame()

        # 2. Normaliza os dados usando as regras do mapping_spec
        df = self._normalize_data(raw_data, mapping_spec, job_name)

        # 3. Continua com o fluxo normal de mapeamento e validação
        df = self._prepare_and_map(df, job_config, mapping_spec, job_name)
        eff_keys = self._get_effective_keys(job_config, mapping_spec)
        df = self._deduplicate(df, eff_keys, job_name)
        self._validate(df, eff_keys, mapping_spec, job_name)
        df = self._transform(df, job_name)
        return df

    def _extract(self, adapter: Any, job_name: str) -> List[Dict]:
        self.log.info(f"[{job_name}] Extraindo dados brutos...")
        t0 = time.perf_counter()
        try:
            # O adapter.extract() agora retorna a lista de dicionários brutos
            raw_data = adapter.extract_raw() 
            self.log.info(f"✅ [{job_name}] Extração concluída: {len(raw_data)} registros brutos em {time.perf_counter()-t0:.2f}s")
            return raw_data
        except Exception as e:
            raise DataExtractionError(f"Falha na extração para o job '{job_name}'.") from e

    def _normalize_data(self, raw_data: List[Dict], mapping_spec: Any, job_name: str) -> pd.DataFrame:
        """
        Normaliza os dados brutos, lidando com listas aninhadas, metadados aninhados
        e conflitos de nome usando um prefixo.
        """
        self.log.info(f"[{job_name}] Normalizando dados...")
        record_path = getattr(mapping_spec, 'record_path', None)
        
        if record_path:
            meta_cols_config = getattr(mapping_spec, 'meta_cols', [])
            meta_prefix_config = getattr(mapping_spec, 'meta_prefix', None)

            processed_meta = [
                col.split('.') if '.' in col else col 
                for col in meta_cols_config
            ]
            
            data_with_records = [
                record for record in raw_data 
                if isinstance(record, dict) and record_path in record and isinstance(record[record_path], list)
            ]

            if not data_with_records:
                self.log.warning(f"[{job_name}] Nenhum registro encontrado com o record_path '{record_path}'. Retornando DataFrame vazio.")
                return pd.DataFrame()

            self.log.info(f"[{job_name}] Encontrados {len(data_with_records)}/{len(raw_data)} registros com o record_path '{record_path}'. Aplicando normalização...")
            
            return pd.json_normalize(
                data_with_records,
                record_path=record_path,
                meta=processed_meta,
                meta_prefix=meta_prefix_config,
                errors='ignore' 
            )
        else:
            return pd.json_normalize(raw_data)

    def _prepare_and_map(self, df: pd.DataFrame, job_config: Any, mapping_spec: Any, job_name: str) -> pd.DataFrame:
        if df.empty: 
            return df
        self.log.info(f"🔧 [{job_name}] Preparando e mapeando dataframe...")
        if not mapping_spec:
            raise DataMatError(f"map_id '{getattr(job_config, 'map_id', 'N/A')}' não foi encontrado no mappings.py")
        
        w = df.copy()
        
        # Lista das colunas de ORIGEM que queremos manter
        expected_src_cols = list(mapping_spec.src_to_tgt.keys())
        
        # Garante que todas as colunas esperadas existam, preenchendo com nulos se faltarem
        for col in expected_src_cols:
            if col not in w.columns:
                self.log.warning(f"[{job_name}] Coluna de origem '{col}' não encontrada. Adicionando com valores nulos.")
                w[col] = None

        # Filtra o DataFrame, mantendo APENAS as colunas que estão no mapeamento.
        # Isso descarta colunas extras como 'caut'.
        w = w[expected_src_cols]
        
        # Renomeia as colunas para o padrão do destino
        w = w.rename(columns=mapping_spec.src_to_tgt)
        
        # Limpeza segura dos dados de texto
        for c in w.select_dtypes(include=['object', 'string']).columns:
            w[c] = w[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
            
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
            schema_cols = {col: pa.Column(**rules) for col, rules in validation_rules.items()}
            for key in keys:
                schema_cols.setdefault(key, pa.Column()).properties.update({'unique': True, 'required': True, 'nullable': False})
            schema = pa.DataFrameSchema(columns=schema_cols, strict=False, coerce=True)
            schema.validate(df, lazy=True)
            self.log.info(f"✅ [{job_name}] Validação de dados concluída.")
        except pa.errors.SchemaErrors as err:
            message = f"Falha na validação de dados:\n{err.failure_cases.to_markdown(index=False)}"
            raise DataValidationError(message) from err

    def _transform(self, df: pd.DataFrame, job_name: str) -> pd.DataFrame:
        # A lógica de transformação genérica permanece a mesma.
        return df

    def _load(self, df: pd.DataFrame, job_config: Any, mapping_spec: Any, job_name: str) -> Tuple[int, int]:
        if df.empty: 
            return 0, 0
        
        table = job_config.table
        schema = os.getenv(job_config.db_name) 
        if not schema:
            raise ValueError(f"A variável de ambiente para o banco de dados '{job_config.db_name}' não foi definida.")
            
        self.log.info(f"🚚 [{job_name}] Carregando {len(df)} linhas para '{schema}.{table}'...")
        
        keys = self._get_effective_keys(job_config, mapping_spec)
        
        try:
            if not keys:
                return self._append_to_db(df, table, schema)
            
            compare_cols = getattr(mapping_spec, "compare_cols", None)
            
            temp_table_prefix = "##" if self.dialect == 'mssql' else ""
            temp_table_name = f"{temp_table_prefix}temp_{job_name.lower().replace(' ', '_')}_{int(time.time())}"
            
            with self.engine.connect() as conn:
                try:
                    with conn.begin() as transaction:
                        df_coerced = self._coerce_df_types_from_db_schema(df, table, schema, conn, job_name)
                        df_coerced.to_sql(temp_table_name.replace("##", ""), conn, if_exists='replace', index=False, schema='tempdb' if self.dialect == 'mssql' else None)
                        
                        self.log.info(f"[{job_name}] Delegando operação de MERGE para a estratégia '{self.strategy.__class__.__name__}'.")
                        inserted, updated = self.strategy.execute_merge(conn, df_coerced, temp_table_name, table, keys, compare_cols, schema)
                        
                        transaction.commit()
                        return inserted, updated
                finally:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
                    
        except Exception as e:
            detailed_error_message = f"Falha na carga para a tabela '{table}'. Erro original: {e}"
            raise DataLoadError(detailed_error_message) from e


    def _append_to_db(self, df: pd.DataFrame, table_name: str, schema: Optional[str]) -> Tuple[int, int]:
        df.to_sql(table_name, con=self.engine, schema=schema, if_exists=self.config.ingest_if_exists, index=False, chunksize=self.config.ingest_chunksize, method=self.config.ingest_method)
        return len(df), 0

    def _coerce_df_types_from_db_schema(self, df: pd.DataFrame, table_name: str, schema: Optional[str], conn: Connection, job_name: str) -> pd.DataFrame:
        # A lógica de coerção de tipos permanece a mesma.
        return df

    def _get_effective_keys(self, job_config: Any, mapping_spec: Any) -> List[str]:
        keys = getattr(mapping_spec, "key_cols", [])
        return [keys] if isinstance(keys, str) else keys
    
    def _save_df_to_excel(self, df: pd.DataFrame, tenant_id: str, job_name: str, root_dir: Path) -> None:
        """
        Salva um DataFrame em um arquivo .xlsx na estrutura de pastas do tenant.
        """
        try:
            output_path = root_dir / "tenants" / tenant_id / "data"
            output_path.mkdir(exist_ok=True)
            safe_job_name = job_name.replace(" ", "_").replace("/", "-")
            export_file = output_path / f"{safe_job_name}.xlsx"
            df.to_excel(export_file, index=False)
            
            self.log.info(f"✅ Dados exportados para: {export_file}")
        except Exception as e:
            self.log.error(f"Falha ao exportar o arquivo para o job '{job_name}': {e}")
            raise DataLoadError(f"Falha ao salvar o arquivo Excel para o job '{job_name}'.") from e