from __future__ import annotations
import importlib
import os
import re
import time
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from core.datamat import DataMat, DataMatConfig
from core.errors.exceptions import DataMatError
from core.adapters.file_adapter import FileSourceAdapter
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("Orchestrator")


# ===================================================================
# Módulo de Configuração
# ===================================================================

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

def _load_env_file(env_path: Path):
    if not env_path.exists(): return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=True)
    except ImportError:
        log.warning("Biblioteca 'python-dotenv' não instalada. Carregando .env manualmente.")
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip('"\'')

def load_environments(client_id: Optional[str]):
    _load_env_file(Path(".env"))
    if client_id:
        client_env_path = Path("tenants") / client_id / "config" / ".env"
        if not client_env_path.exists():
            raise FileNotFoundError(f"Arquivo de configuração do cliente não encontrado: {client_env_path}")
        _load_env_file(client_env_path)

def expand_placeholders(value: Any) -> Any:
    if not isinstance(value, str): return value
    return _VAR_PATTERN.sub(lambda m: os.getenv(m.group(1), m.group(0)), value)


# ===================================================================
# Módulo de Fábricas (Factories)
# ===================================================================

_engine_cache: Dict[str, Engine] = {}

def get_engine(db_name_var: str) -> Engine:
    db_name = os.getenv(db_name_var, db_name_var)
    if db_name in _engine_cache:
        return _engine_cache[db_name]
    url_template = os.getenv("DB_URL")
    if not url_template or "{db}" not in url_template:
        raise RuntimeError("Variável de ambiente DB_URL com placeholder {db} não está definida.")
    engine = create_engine(url_template.format(db=db_name), pool_pre_ping=True)
    _engine_cache[db_name] = engine
    return engine

def build_adapter_for_job(job: Any, preview_rows: Optional[int]) -> Any:
    job_type = getattr(job, "type")
    log.info(f"[{job.name}] Construindo adapter do tipo '{job_type}'...")
    if job_type == "file":
        return FileSourceAdapter(file_path=expand_placeholders(job.file), sheet=job.sheet)
    if job_type == "api":
        return APISourceAdapter(
            endpoint=expand_placeholders(job.endpoint),
            paging=getattr(job, "paging", None),
            auth=getattr(job, "auth", None),
            params=getattr(job, "params", None),
            enrich_by_id=getattr(job, "enrich_by_id", False),
            enrichment_strategy=getattr(job, "enrichment_strategy", 'concurrent'),
            row_limit=preview_rows,
            data_path=getattr(job, "data_path", None),
            detail_data_path=getattr(job, "detail_data_path", None),
            requests_per_minute=getattr(job, "requests_per_minute", 60),
            enrichment_requests_per_minute=getattr(job, "enrichment_requests_per_minute", None)
        )
    if job_type == "db":
        return DatabaseSourceAdapter(source_url=expand_placeholders(job.source_url), query=expand_placeholders(job.query))
    raise ValueError(f"Tipo de job desconhecido: '{job_type}'")

def build_datamat_config() -> DataMatConfig:
    return DataMatConfig(
        ingest_if_exists=os.getenv("INGEST_IF_EXISTS", "append"),
        ingest_chunksize=int(os.getenv("INGEST_CHUNKSIZE", "2000")),
        ingest_method=os.getenv("INGEST_METHOD", "multi"),
        etl_log_table=os.getenv("ETL_LOG_TABLE", "tbInfra_LogCarga")
    )


# ===================================================================
# Módulo de Execução (Runner)
# ===================================================================

def run_single_job(dm: DataMat, job: Any, mappings: Dict, preview: bool, export_path: Optional[Path], export_rows: Optional[int]):
    adapter = build_adapter_for_job(job, export_rows)
    if preview or export_path:
        log.info(f"[{job.name}] Executando em modo de depuração...")
        df = adapter.extract()
        if export_path:
            export_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_excel(export_path, index=False)
            log.info(f"📦 [{job.name}] Dados exportados para: {export_path}")
        if preview:
            print(f"\n--- PREVIEW: {job.name} ({len(df)} linhas) ---\n{df.head(export_rows or 5).to_markdown(index=False)}\n")
        return job.name, 0, 0
    return dm.run_etl_job(adapter, job, mappings.get(job.map_id))

def run_client_pipeline(client_id: str, job_filter: Optional[str], preview: bool, export_path: Optional[str], export_rows: Optional[int]):
    log.info(f"Iniciando pipeline para o cliente: {client_id}.")
    load_environments(client_id)

    jobs_mod = importlib.import_module(f"tenants.{client_id}.pipelines.jobs")
    mappings_mod = importlib.import_module(f"tenants.{client_id}.pipelines.mappings")
    jobs_list = [j for j in getattr(jobs_mod, "JOBS") if not job_filter or j.name == job_filter]
    if job_filter and not jobs_list:
        raise ValueError(f"Job '{job_filter}' não encontrado.")

    dm_config = build_datamat_config()
    dm_instances: Dict[str, DataMat] = {}
    
    def get_dm_instance(db_var: str) -> DataMat:
        if db_var not in dm_instances:
            dm_instances[db_var] = DataMat(get_engine(db_var), dm_config)
        return dm_instances[db_var]

    dw_db_var = os.getenv("DB_DW_NAME")
    if not dw_db_var:
        raise RuntimeError("DB_DW_NAME não definido no .env para operações de log e DW.")
    dm_dw_logger = get_dm_instance(dw_db_var)

    stg_results: List[Tuple[str, int, int]] = []

    # Verifica o "interruptor" de paralelismo no .env
    parallel_enabled = os.getenv("ETL_PARALLEL_EXECUTION", "true").lower() in ("true", "1", "yes", "on")

    if parallel_enabled and not job_filter and not preview and not export_path:
        log.info("🚀 Executando jobs de STG em modo PARALELO.")
        with ThreadPoolExecutor(max_workers=int(os.getenv("MAX_WORKERS", "2"))) as pool:
            f_path = Path(f"tenants/{client_id}/data/{export_path}") if export_path else None
            futures = {
                pool.submit(run_single_job, get_dm_instance(job.db_name), job, getattr(mappings_mod, "MAPPINGS"), preview, f_path, export_rows): job
                for job in jobs_list
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    stg_results.append(future.result())
                except Exception as e:
                    log.error(f"📦 [STG] FALHA CRÍTICA no job '{job.name}': {e}", exc_info=True)
                    dm_dw_logger.log_etl_error(process_name=job.name, message=str(e))
                    stg_results.append((job.name, -1, -1))
    else:
        log.info("🐌 Executando jobs de STG em modo SEQUENCIAL (um a um).")
        f_path = Path(f"tenants/{client_id}/data/{export_path}") if export_path else None
        for job in jobs_list:
            try:
                result = run_single_job(get_dm_instance(job.db_name), job, getattr(mappings_mod, "MAPPINGS"), preview, f_path, export_rows)
                stg_results.append(result)
            except Exception as e:
                log.error(f"📦 [STG] FALHA CRÍTICA no job '{job.name}': {e}", exc_info=True)
                dm_dw_logger.log_etl_error(process_name=job.name, message=str(e))
                stg_results.append((job.name, -1, -1))

    if preview or export_path:
        log.info("✅ Execução em modo de depuração/filtro concluída.")
        return

    dw_objects_list = getattr(jobs_mod, "DW_OBJECTS", [])
    if dw_objects_list:
        dw_base_path = Path(f"tenants/{client_id}/dw")
        # O synchronize_dw_objects agora é chamado na instância do DW Logger, que é a principal
        dm_dw_logger.synchronize_dw_objects(dw_base_path, dw_objects_list)
    
    proc_results: List[Tuple[int, int]] = []
    for proc_group in getattr(jobs_mod, "PROCS", []):
        log.info("▶️  [PROC] Executando grupo de procedures...")
        for proc_info in proc_group:
            target_db_var = proc_info.get("db_name")
            if not target_db_var:
                raise ValueError(f"Procedure '{proc_info['sql']}' não tem um 'db_name' definido.")
            
            dm_instance = get_dm_instance(target_db_var)
            proc_results.append(dm_instance.run_dw_procedure(proc_info["sql"]))

    DataMat.log_summary(client_id, stg_results, proc_results)


# ===================================================================
# Ponto de Entrada da Aplicação
# ===================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador de ETL para DataMat.")
    parser.add_argument("client_id", help="ID do cliente a ser processado.")
    parser.add_argument("--job", help="Executa apenas um job específico.")
    parser.add_argument("--preview", action="store_true", help="Ativa o modo preview (requer --job).")
    parser.add_argument("--export", help="Ativa o modo de exportação para um arquivo XLSX (requer --job).")
    parser.add_argument("--rows", type=int, help="Limita o número de linhas para --preview e --export.")
    args = parser.parse_args()

    if (args.preview or args.export) and not args.job:
        parser.error("--preview e --export requerem que --job seja especificado.")

    try:
        t_start = time.perf_counter()
        run_client_pipeline(
            client_id=args.client_id,
            job_filter=args.job,
            preview=args.preview,
            export_path=args.export,
            export_rows=args.rows
        )
        log.info(f"🏁 Pipeline para '{args.client_id}' finalizado em {time.perf_counter() - t_start:.2f} segundos.")
    except (DataMatError, ValueError, FileNotFoundError) as e:
        log.error(f"\n❌ ERRO CONTROLADO: A execução falhou. Causa: {e}")
    except Exception as e:
        log.error(f"\n❌ ERRO INESPERADO: A execução foi abortada. Causa: {e}", exc_info=True)