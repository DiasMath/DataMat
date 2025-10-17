import argparse
from datetime import datetime, timedelta
import importlib
import logging.config
import os
import sys
from pathlib import Path
import traceback
from typing import Any, List

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine



# Importa as classes necessárias dos módulos do seu projeto
from core.datamat import DataMat, DataMatConfig
from core.errors.exceptions import DataMatError
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter
from core.adapters.file_adapter import FileSourceAdapter

# Adiciona o diretório raiz ao path para permitir imports absolutos
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

# --- CONFIGURAÇÃO DE LOGGING (CORRIGIDA) ---
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            # A linha 'encoding' foi REMOVIDA daqui, pois não é suportada pelo StreamHandler.
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": "datamat.log",
            "mode": "a",
            "encoding": "utf-8"  # Mantida aqui, pois é correta para arquivos.
        },
    },
    "root": {"handlers": ["console", "file"], "level": "INFO"},
}
logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger(__name__)


def get_db_engine() -> Engine:
    """
    Cria uma engine do SQLAlchemy a partir da DB_URL e de configurações
    avançadas de pool de conexão definidas no .env.
    """
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("A variável de ambiente 'DB_URL' não está definida. Verifique seus arquivos .env.")

    # Substitui o placeholder {db} se ele existir, para compatibilidade
    if '{db}' in db_url:
        # Usa um nome de banco de dados padrão para o log, já que a engine se conecta ao servidor
        db_name_for_log = os.getenv("DB_DW_NAME", "default_db")
        db_url = db_url.format(db=db_name_for_log)

    pool_options = {
        "pool_size": int(os.getenv("DB_POOL_SIZE", 5)),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", 10)),
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", 1800)),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", 30)),
        "echo": os.getenv("DB_ECHO", "0") == "1"
    }

    try:
        return create_engine(db_url, **pool_options)
    except Exception as e:
        log.critical(f"Falha fatal ao criar a engine do banco de dados: {e}")
        raise

def get_job_adapter(job_spec: Any, limit: int = 0) -> Any:
    """
    Cria a instância do adapter correto, passando o limite de linhas
    para o construtor, quando aplicável.
    """
    if job_spec.type == "api":
        return APISourceAdapter(
            endpoint=getattr(job_spec, 'endpoint', None),
            auth=getattr(job_spec, 'auth', None),
            paging=getattr(job_spec, 'paging', None),
            enrich_by_id=getattr(job_spec, 'enrich_by_id', False),
            data_path=getattr(job_spec, 'data_path', None),
            detail_data_path=getattr(job_spec, 'detail_data_path', None),
            enrichment_strategy=getattr(job_spec, 'enrichment_strategy', 'concurrent'),
            requests_per_minute=getattr(job_spec, 'requests_per_minute', None),
            enrichment_requests_per_minute=getattr(job_spec, 'enrichment_requests_per_minute', None),
            params=getattr(job_spec, 'params', None),
            param_matrix=getattr(job_spec, 'param_matrix', None),
            delay_between_pages_ms=getattr(job_spec, 'delay_between_pages_ms', None),
            row_limit=limit if limit > 0 else getattr(job_spec, 'row_limit', None)
        )
    elif job_spec.type == 'file':
        file_path = getattr(job_spec, 'file', None)
        if not file_path:
            raise ValueError(f"Job '{job_spec.name}' is of type 'file' but no 'file' path was specified.")

        return FileSourceAdapter(
            path=file_path,
            sheet=getattr(job_spec, 'sheet', None),
            header=getattr(job_spec, 'header', 0),
            delimiter=getattr(job_spec, 'delimiter', ';')
        )
    elif job_spec.type == "db":
        return DatabaseSourceAdapter(
            source_url=getattr(job_spec, 'source_url', None),
            query=getattr(job_spec, 'query', None),
            params=getattr(job_spec, 'params', None)
        )
    raise ValueError(f"Tipo de job '{job_spec.type}' não suportado.")

def run_tenant_pipeline(
    tenant_id: str,
    job_names: List[str] = None,
    preview: bool = False,
    export: bool = False,
    limit: int = 0,
    workers_per_client: int = 2,
    procs_only: bool = False
):
    """
    Executa o pipeline para um tenant, respeitando os contratos de chamada e retorno.
    """
    log.info(f"================ INICIANDO PIPELINE PARA O TENANT: {tenant_id} ================")
    
    stg_results = []
    total_rows_pipeline = 0

    try:
        # Lógica de carregamento de configuração (permanece a mesma)
        env_path = ROOT_DIR / "tenants" / tenant_id / "config" / ".env"
        if not env_path.exists():
            log.error(f"Arquivo .env não encontrado para o tenant '{tenant_id}' em {env_path}")
            return -1, {"error": f"Arquivo .env não encontrado para o tenant '{tenant_id}'"}
        load_dotenv(dotenv_path=env_path)

        global_env_path = ROOT_DIR / ".env"
        if global_env_path.exists():
            load_dotenv(dotenv_path=global_env_path)

        jobs_module = importlib.import_module(f"tenants.{tenant_id}.pipelines.jobs")
        mappings_module = importlib.import_module(f"tenants.{tenant_id}.pipelines.mappings")
        ALL_JOBS = getattr(jobs_module, "JOBS")
        PROCS = getattr(jobs_module, "PROCS", [])
        MAPPINGS = getattr(mappings_module, "MAPPINGS")

        jobs_to_run = ALL_JOBS
        if job_names:
            jobs_to_run = [j for j in ALL_JOBS if j.name in job_names]
            log.info(f"Executando jobs específicos: {[j.name for j in jobs_to_run]}")

        engine = get_db_engine()

        datamat_config = DataMatConfig(
            ingest_if_exists=os.getenv("INGEST_IF_EXISTS", "append"),
            ingest_chunksize=int(os.getenv("INGEST_CHUNKSIZE", 2000)),
            ingest_method=os.getenv("INGEST_METHOD", "multi"),
            etl_log_table=os.getenv("ETL_LOG_TABLE", "tbInfra_LogCarga"),
        )
        datamat = DataMat(engine=engine, config=datamat_config)

        # ALTERAÇÃO: A execução dos jobs de ETL agora é condicional
        if not procs_only:
            log.info("Iniciando execução dos jobs de STG...")
            for job_spec in jobs_to_run:
                try:
                    # Lógica incremental (permanece a mesma)
                    inc_config = getattr(job_spec, 'incremental_config', None)
                    if inc_config and inc_config.get("enabled", False):
                        days = inc_config.get("days_to_load", 30)
                        param_start = inc_config.get("date_param_start")
                        param_end = inc_config.get("date_param_end")
                        if param_start and param_end:
                            end_date = datetime.now()
                            start_date = end_date - timedelta(days=days)
                            date_format = "%Y-%m-%d"
                            if job_spec.params is None: 
                                job_spec.params = {}
                            job_spec.params[param_start] = start_date.strftime(date_format)
                            job_spec.params[param_end] = end_date.strftime(date_format)
                            log.info(f"[{job_spec.name}] Carga incremental ativada. Carregando dados de {days} dias.")

                    effective_limit = limit if preview or export else 0
                    adapter = get_job_adapter(job_spec, limit=effective_limit)
                    mapping_spec = MAPPINGS.get(job_spec.map_id)
                    
                    if export:
                        datamat.export_job_to_excel(adapter, job_spec, mapping_spec, tenant_id, ROOT_DIR, limit)
                        continue
                    
                    if preview:
                        log.info(f"Executando em modo PREVIEW para o job '{job_spec.name}'")
                        df = datamat.run_etl_job_extract_only(adapter, job_spec, mapping_spec)
                        print(f"\n--- Preview do Job: {job_spec.name} ---")
                        print(df.head(limit))
                        continue

                    result_tuple = datamat.run_etl_job(adapter, job_spec, mapping_spec)
                    stg_results.append(result_tuple)
                    
                    inserted_rows = result_tuple[1]
                    updated_rows = result_tuple[2]
                    total_rows_pipeline += inserted_rows + updated_rows

                except (DataMatError, Exception) as e:
                    log.critical(f"Job '{job_spec.name}' falhou com erro inesperado: {e}", exc_info=True)
                    datamat.log_etl_error(process_name=job_spec.name, message=str(e))
                    raise 
        else:
            log.info("Flag '--procs-only' ativa. Pulando a execução dos jobs de STG.")

        proc_results = []
        # ALTERAÇÃO: A lógica agora permite a execução de procedures de forma isolada
        if procs_only or not (job_names or preview or export):
            if PROCS:
                log.info("Iniciando execução das procedures do DW...")
                for group in PROCS:
                    for proc in group:
                        proc_results.append(datamat.run_dw_procedure(proc))
            else:
                log.info("Nenhuma procedure definida para execução.")
        else:
            log.info("Procedures não executadas devido aos parâmetros de execução (--jobs, --preview, --export).")
        
        datamat.log_summary(tenant_id, stg_results, proc_results)

    except Exception as e:
        log.critical(f"Erro inesperado no pipeline do tenant '{tenant_id}': {e}", exc_info=True)
        return -1, {"error": str(e), "traceback": traceback.format_exc()}
    
    log.info(f"================ FINALIZANDO PIPELINE PARA O TENANT: {tenant_id} ================")
    
    return total_rows_pipeline, stg_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de pipelines de ETL do DataMat.")
    parser.add_argument("tenants", nargs='*', help="IDs dos tenants. Se vazio, executa todos.")
    parser.add_argument("-j", "--jobs", nargs='+', help="Nomes dos jobs específicos.")
    parser.add_argument("-p", "--preview", action="store_true", help="Modo de preview.")
    parser.add_argument("-e", "--export", action="store_true", help="Exporta para CSV.")
    parser.add_argument("-l", "--linhas", type=int, default=10, help="Linhas para o preview.")
    parser.add_argument("-po","--procs-only", action="store_true", help="Executa apenas as procedures do DW.")
    args = parser.parse_args()
    
    tenants_to_run = args.tenants
    if not tenants_to_run:
        all_tenants = [d.name for d in (ROOT_DIR / "tenants").iterdir() if d.is_dir() and not d.name.startswith('_')]
        tenants_to_run = [t for t in all_tenants if (ROOT_DIR / "tenants" / t / "config" / ".env").exists()]
        if tenants_to_run:
            log.info(f"Nenhum tenant especificado. Executando para todos os configurados: {tenants_to_run}")
        else:
            log.warning("Nenhum tenant com arquivo .env foi encontrado para executar.")

    for tenant in tenants_to_run:
        run_tenant_pipeline(
            tenant_id=tenant,
            job_names=args.jobs,
            preview=args.preview,
            export=args.export,
            limit=args.linhas,
            procs_only=args.procs_only
        )