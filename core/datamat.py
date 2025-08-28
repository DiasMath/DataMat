from __future__ import annotations
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandera as pa
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# JORGE SAYS: Adicionado o import do logging que estava em falta.
log = logging.getLogger(__name__)

class DataMat:
    """
    Classe principal para interação com o banco de dados e execução de lógicas de ETL.
    Atua como a "biblioteca de ferramentas" para o orquestrador.
    """
    class DataMatError(Exception):
        pass

    def __init__(self, engine: Engine, ingest_cfg: Dict) -> None:
        self.engine = engine
        self.ingest_cfg = ingest_cfg
        self.log = logging.getLogger(f"DataMat.{engine.url.database}")

    def _is_mysql(self) -> bool:
        return self.engine.dialect.name.lower() == "mysql"

    def execute_sql_script(self, script_content: str) -> None:
        """Executa um script SQL que pode conter múltiplos comandos e delimitadores."""
        with self.engine.connect() as conn:
            # Remove comentários
            script_content = re.sub(r'--.*?\n', '', script_content)
            
            # Lida com delimitadores para executar scripts complexos
            delimiters = re.findall(r'DELIMITER\s+([^\s]+)', script_content, re.IGNORECASE)
            delimiters.insert(0, ';') # Delimitador inicial é sempre ;

            script_parts = re.split(r'DELIMITER\s+[^\s]+', script_content, flags=re.IGNORECASE)

            statements = []
            for i, part in enumerate(script_parts):
                delimiter = delimiters[i]
                # Divide cada parte pelo seu delimitador específico
                sub_statements = [s.strip() for s in part.split(delimiter) if s.strip()]
                statements.extend(sub_statements)

            # Executa cada statement numa transação
            if statements:
                with conn.begin():
                    for stmt in statements:
                        if stmt:
                            conn.execute(text(stmt))

    def log_etl_error(self, process_name: str, message: str) -> None:
        """Registra uma falha na tabela de log do DW."""
        try:
            log_table = os.getenv("ETL_LOG_TABLE", "tbInfra_LogCarga")
            error_message = f"ERRO: {message[:65000]}"
            sql = text(f"INSERT INTO {log_table} (NomeProcedure, Mensagem, LinhasAfetadas) VALUES (:name, :msg, 0)")
            with self.engine.begin() as conn:
                conn.execute(sql, {"name": process_name, "msg": error_message})
        except Exception as e:
            self.log.error("FALHA AO REGISTRAR O ERRO NO BANCO: %s", e)

    def to_db(self, df: pd.DataFrame, table_name: str, schema: Optional[str] = None) -> Tuple[int, int]:
        """Carrega um DataFrame para uma tabela usando o método 'append'."""
        if df.empty:
            return 0, 0
        df.to_sql(
            table_name,
            con=self.engine,
            schema=schema or self.ingest_cfg.get("schema"),
            if_exists=self.ingest_cfg.get("if_exists", "append"),
            index=False,
            chunksize=self.ingest_cfg.get("chunksize", 2000),
            method=self.ingest_cfg.get("method", "multi")
        )
        return len(df), 0
        
    def merge_into_mysql(
        self, df: pd.DataFrame, table_name: str, key_cols: List[str],
        compare_cols: Optional[List[str]] = None,
        schema: Optional[str] = None, job_name: str = "unknown"
    ) -> Tuple[int, int]:
        """Executa uma operação de UPSERT (update/insert) em uma tabela MySQL."""
        if df.empty:
            self.log.info("[%s] DataFrame vazio, nenhuma ação de merge necessária para a tabela '%s'.", job_name, table_name)
            return 0, 0
        if not key_cols:
            raise self.DataMatError("merge_into_mysql requer pelo menos uma key_col.")

        with self.engine.connect() as conn:
            trans = conn.begin()
            temp_table_name = f"temp_{job_name.lower().replace(' ', '_')}_{int(time.time())}"
            try:
                # 1. Obter o schema da tabela de destino para garantir a consistência dos tipos
                inspector = inspect(self.engine)
                columns_info = {c['name']: c['type'] for c in inspector.get_columns(table_name, schema=schema)}

                # 2. Forçar os tipos de dados do DataFrame para corresponderem à tabela de destino
                for col_name, col_type in columns_info.items():
                    if col_name in df.columns:
                        try:
                            col_type_str = str(col_type).lower()
                            if "int" in col_type_str:
                                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                            elif "datetime" in col_type_str:
                                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                            elif any(t in col_type_str for t in ["decimal", "float", "double"]):
                                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                        except Exception as e:
                            self.log.warning(f"[{job_name}] Não foi possível converter a coluna '{col_name}' para o tipo '{col_type}': {e}")
                
                # 3. Criar e carregar a tabela temporária
                df.to_sql(temp_table_name, conn, if_exists='replace', index=False)
                
                cols_to_compare = compare_cols if compare_cols is not None else [c for c in df.columns if c not in key_cols]
                
                target_fullname = f"`{schema}`.`{table_name}`" if schema else f"`{table_name}`"
                temp_fullname = f"`{temp_table_name}`"
                
                join_conditions = " AND ".join([f"dw.`{key}` = tmp.`{key}`" for key in key_cols])
                
                # 4. Lógica de UPDATE
                updated_rows = 0
                if cols_to_compare:
                    update_set_clauses = ", ".join([f"dw.`{col}` = tmp.`{col}`" for col in cols_to_compare])
                    update_sql = f"UPDATE {target_fullname} dw JOIN {temp_fullname} tmp ON {join_conditions} SET {update_set_clauses}"
                    
                    where_clauses = " OR ".join([f"dw.`{col}` <=> tmp.`{col}` IS NOT TRUE" for col in cols_to_compare])
                    update_sql += f" WHERE {where_clauses};"
                    
                    result_update = conn.execute(text(update_sql))
                    updated_rows = result_update.rowcount

                # 5. Lógica de INSERT
                all_cols = ", ".join([f"`{col}`" for col in df.columns])
                select_cols = ", ".join([f"tmp.`{col}`" for col in df.columns])
                
                insert_sql = f"""
                    INSERT INTO {target_fullname} ({all_cols})
                    SELECT {select_cols}
                    FROM {temp_fullname} tmp
                    LEFT JOIN {target_fullname} dw ON {join_conditions}
                    WHERE dw.`{key_cols[0]}` IS NULL;
                """
                
                result_insert = conn.execute(text(insert_sql))
                inserted_rows = result_insert.rowcount

                # 6. Limpeza e commit
                conn.execute(text(f"DROP TABLE {temp_fullname};"))
                trans.commit()
                
                return inserted_rows, updated_rows
            
            except Exception:
                trans.rollback()
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_fullname};"))
                except: pass
                raise

    def prepare_dataframe(
        self, df: pd.DataFrame, columns: Optional[List[str]] = None,
        rename_map: Optional[Dict[str, str]] = None,
        required: Optional[List[str]] = None,
        drop_extra: bool = True, strip_strings: bool = True
    ) -> pd.DataFrame:
        """Prepara um DataFrame para carga: seleciona, renomeia e limpa colunas."""
        if df.empty:
            return pd.DataFrame(columns=list(rename_map.values()) if rename_map else (columns or []))

        w = df.copy()
        if columns is not None:
            missing = [c for c in columns if c not in w.columns]
            if missing:
                raise self.DataMatError(f"Colunas ausentes na origem: {missing}")
            w = w[list(columns)]
        
        if rename_map:
            w = w.rename(columns=rename_map)
        
        if drop_extra and columns is None and rename_map:
            keep = set(rename_map.values())
            w = w[[c for c in w.columns if c in keep]]
        
        if required:
            miss = [c for c in required if c not in w.columns]
            if miss:
                raise self.DataMatError(f"Colunas obrigatórias ausentes: {miss}")
        
        if strip_strings:
            for c in w.columns:
                if pd.api.types.is_string_dtype(w[c]) or pd.api.types.is_object_dtype(w[c]):
                    try:
                        w[c] = w[c].str.strip()
                    except AttributeError:
                        pass
        
        return w

    def run_etl_job(self, adapter: Any, job_config: Any, mapping_spec: Any) -> Tuple[str, int, int]:
        """
        Executa a sequência completa de um job de ETL:
        Extração -> Preparação -> Deduplicação -> Validação -> Carga.
        """
        job_name = job_config.name
        log.info(f"▶️  [{job_name}] Iniciando job...")
        t_job0 = time.perf_counter()

        try:
            # 1. EXTRAÇÃO
            t0 = time.perf_counter()
            df: pd.DataFrame = adapter.extract()
            log.info(f"✅ [{job_name}] Extração concluída: {len(df)} linhas em {time.perf_counter()-t0:.2f}s")

            # 2. PREPARAÇÃO E MAPEAMENTO
            spec = mapping_spec
            if not spec and getattr(job_config, "map_id", None):
                 raise self.DataMatError(f"map_id '{job_config.map_id}' não foi encontrado no arquivo mappings.py")

            eff_keys = getattr(job_config, "key_cols", None) or (spec.key_cols if spec else [])
            if isinstance(eff_keys, (str, bytes)): eff_keys = [eff_keys]

            log.info(f"🔧 [{job_name}] Preparando dataframe...")
            df = self.prepare_dataframe(
                df,
                columns=getattr(job_config, "columns", None) or (list(spec.src_to_tgt.keys()) if spec else None),
                rename_map=getattr(job_config, "rename_map", None) or (spec.src_to_tgt if spec else None),
                required=getattr(job_config, "required", None) or (spec.required if spec else None),
                drop_extra=True,
                strip_strings=True
            )
            log.info(f"✅ [{job_name}] Dataframe preparado: {len(df)} linhas")

            # 3. DEDUPLICAÇÃO
            if eff_keys:
                log.info(f"🧹 [{job_name}] Removendo duplicatas por chave {eff_keys} ...")
                t4, before = time.perf_counter(), len(df)
                df = df.drop_duplicates(subset=eff_keys, keep='last').reset_index(drop=True)
                log.info(f"✅ [{job_name}] Dedup: {before}->{len(df)} em {time.perf_counter()-t4:.2f}s")

            # 4. VALIDAÇÃO
            eff_rules = spec.validation_rules if spec else {}
            if eff_rules or eff_keys:
                log.info(f"🔎 [{job_name}] Validando qualidade dos dados...")
                try:
                    validation_columns = {
                        col_name: pa.Column(**rules)
                        for col_name, rules in eff_rules.items()
                    }
                    for key_col in eff_keys:
                        if key_col not in validation_columns:
                            validation_columns[key_col] = pa.Column(unique=True)
                        else:
                            if 'unique' not in validation_columns[key_col].properties:
                                 validation_columns[key_col].properties['unique'] = True

                    if validation_columns:
                        schema = pa.DataFrameSchema(columns=validation_columns, strict=False, coerce=True)
                        schema.validate(df, lazy=True)
                        log.info(f"✅ [{job_name}] Validação de dados concluída com sucesso.")

                except pa.errors.SchemaErrors as err:
                    error_summary = err.failure_cases.to_markdown()
                    detailed_message = f"Falha na validação de dados para o job '{job_name}':\n{error_summary}"
                    raise self.DataMatError(detailed_message)

            # 5. TRANSFORMAÇÕES ADICIONAIS
            log.info(f"💰 [{job_name}] Normalizando colunas monetárias (heurística)...")
            def _normalize_money_col(s: pd.Series) -> pd.Series:
                if s.dtype.kind in ("i", "u", "f"): return s.round(2)
                txt = (s.astype(str).str.replace(r"\s", "", regex=True).str.replace("R$", "", regex=False)
                     .str.replace(".", "", regex=False, fixed=True).str.replace(",", ".", regex=False, fixed=True))
                return pd.to_numeric(txt, errors="coerce").round(2)
            money_candidates = [c for c in df.columns if any(tok in c.lower() for tok in ("valor", "preco", "preço", "custo", "total"))]
            for c in money_candidates: df[c] = _normalize_money_col(df[c])
            
            # 6. CARGA
            log.info(f"🚚 [{job_name}] Carregando no destino '{job_config.table}' ...")
            if eff_keys and self._is_mysql():
                inserted, updated = self.merge_into_mysql(
                    df, job_config.table, key_cols=eff_keys,
                    compare_cols=getattr(job_config, "compare_cols", None) or (spec.compare_cols if spec else None),
                    schema=getattr(job_config, "schema", None),
                    job_name=job_name
                )
            else:
                inserted, updated = self.to_db(df, job_config.table, schema=getattr(job_config, "schema", None))
            
            log.info(f"🎉 [{job_name}] Carga concluída: {inserted} inseridos, {updated} atualizados.")
            log.info(f"⏱️  [{job_name}] Tempo total do job: {time.perf_counter()-t_job0:.2f}s")
            
            return job_name, inserted, updated

        except Exception as e:
            log.error(f"❌ [{job_name}] Falha na carga: {e}", exc_info=True)
            self.log_etl_error(process_name=job_name, message=str(e))
            raise