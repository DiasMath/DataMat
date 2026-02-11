import argparse
from datetime import datetime, timedelta
import importlib
import logging.config
import os
import sys
from pathlib import Path
import traceback
from typing import Any, List, Tuple, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Adiciona o diretório raiz ao path para permitir imports absolutos
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

# Imports internos
from core.env import load_tenant_env
from core.alerts import observer
from core.datamat import DataMat, DataMatConfig
from core.errors.exceptions import DataMatError, AuthenticationError, ConfigurationError
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter
from core.adapters.file_adapter import FileSourceAdapter


# --- CONFIGURAÇÃO DE LOGGING ---
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(levelname)s - %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": "datamat.log",
            "mode": "a",
            "encoding": "utf-8"
        },
    },
    "root": {"handlers": ["console", "file"], "level": os.getenv("LOG_LEVEL", "INFO")},
}
logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger("DataMat.Main")


def get_db_engine() -> Engine:
    """
    Cria uma engine do SQLAlchemy a partir da DB_URL e de configurações
    avançadas de pool de conexão definidas no .env.
    """
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ConfigurationError("A variável de ambiente 'DB_URL' não está definida. Verifique seus arquivos .env.")

    if '{db}' in db_url:
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
    Fábrica que cria a instância do adapter correto baseado no tipo do job.
    """
    job_type = getattr(job_spec, 'type', 'unknown')
    
    if job_type == "api":
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
            max_passes=getattr(job_spec, 'max_passes', 1),
            row_limit=limit if limit > 0 else getattr(job_spec, 'row_limit', None),
            id_key=getattr(job_spec, 'id_key', 'id')
        )
    elif job_type == 'file':
        file_path = getattr(job_spec, 'file', None)
        if not file_path:
            raise ConfigurationError(f"Job '{job_spec.name}' é do tipo 'file' mas o caminho 'file' não foi especificado.")

        return FileSourceAdapter(
            path=file_path,
            sheet=getattr(job_spec, 'sheet', None),
            header=getattr(job_spec, 'header', 0),
            delimiter=getattr(job_spec, 'delimiter', ';')
        )
    elif job_type == "db":
        return DatabaseSourceAdapter(
            source_url=getattr(job_spec, 'source_url', None),
            query=getattr(job_spec, 'query', None),
            params=getattr(job_spec, 'params', None)
        )
    
    raise ConfigurationError(f"Tipo de job '{job_type}' não suportado ou não definido para o job '{job_spec.name}'.")


def run_tenant_pipeline(
    tenant_id: str,
    job_names: List[str] = None,
    preview: bool = False,
    export: bool = False,
    limit: int = 0,
    workers_per_client: int = 2,
    procs_only: bool = False
) -> Union[Tuple[int, List, List[str]], Tuple[int, dict, List[str]]]:
    """
    Executa o pipeline para um tenant.
    """
    log.info(f"================ INICIANDO PIPELINE PARA O TENANT: {tenant_id} ================")
    
    stg_results = []
    executed_proc_names = []
    total_rows_pipeline = 0

    try:
        # 1. Carrega Variáveis de Ambiente
        if not load_tenant_env(tenant_id):
            msg = f"Configuração (.env) não encontrada ou inválida para '{tenant_id}'"
            observer.notify_failure(tenant_id, "Setup de Ambiente", Exception(msg))
            return -1, {"error": msg}, []

        # 2. Importação Dinâmica
        try:
            jobs_module = importlib.import_module(f"tenants.{tenant_id}.pipelines.jobs")
            mappings_module = importlib.import_module(f"tenants.{tenant_id}.pipelines.mappings")
        except ModuleNotFoundError as e:
            raise ConfigurationError(f"Pipeline não encontrado para {tenant_id}. Verifique a pasta tenants/.") from e

        ALL_JOBS = getattr(jobs_module, "JOBS", [])
        PROCS = getattr(jobs_module, "PROCS", [])
        MAPPINGS = getattr(mappings_module, "MAPPINGS", {})

        jobs_to_run = ALL_JOBS
        if job_names:
            jobs_to_run = [j for j in ALL_JOBS if j.name in job_names]
            log.info(f"Executando jobs específicos: {[j.name for j in jobs_to_run]}")

        # 3. Inicializa Infraestrutura
        engine = get_db_engine()
        datamat_config = DataMatConfig(
            ingest_if_exists=os.getenv("INGEST_IF_EXISTS", "append"),
            ingest_chunksize=int(os.getenv("INGEST_CHUNKSIZE", 2000)),
            ingest_method=os.getenv("INGEST_METHOD", "multi"),
            etl_log_table=os.getenv("ETL_LOG_TABLE", "tbInfra_LogCarga"),
        )
        datamat = DataMat(engine=engine, config=datamat_config)

        # 4. Execução dos Jobs de Ingestão (STG)
        if not procs_only:
            log.info("Iniciando execução dos jobs de STG...")
            
            for job_spec in jobs_to_run:
                try:
                    is_full_load_mode = False

                    # --- A. VERIFICAÇÃO DE "DIA DE CARGA ESPECIAL" (Scheduled Mode) ---
                    # Se hoje for o dia agendado (ex: Domingo), ativamos o modo "Full Load Mode".
                    # O QUE MUDOU: Não forçamos mais truncate=True aqui. Respeitamos o que está no jobs.py.
                    # Se job.truncate for False (padrão), faremos um MERGE/UPSERT da janela estática (ex: Ano Atual)
                    # sem apagar o histórico antigo (ex: Anos Anteriores).
                    if getattr(job_spec, 'full_load_weekday', None) is not None:
                        today_wd = datetime.now().weekday()
                        if today_wd == job_spec.full_load_weekday:
                            log.info(f"📅 [{job_spec.name}] Hoje é dia de Carga Agendada (Dia {today_wd}).")
                            log.info("   -> O cálculo incremental automático será IGNORADO.")
                            log.info("   -> Serão usados os 'params' estáticos definidos no jobs.py.")
                            
                            # [IMPORTANTE] Removemos a linha: job_spec.truncate = True
                            # Agora o truncate só acontece se estiver explícito no jobs.py: Job(..., truncate=True)
                            
                            is_full_load_mode = True

                    # --- B. LÓGICA DE CARGA INCREMENTAL (Dinâmica) ---
                    # Só roda se NÃO estivermos no modo Agendado/Full Load
                    inc_config = getattr(job_spec, 'incremental_config', None)
                    
                    if inc_config and inc_config.get("enabled", False) and not is_full_load_mode:
                        days = inc_config.get("days_to_load", 30)
                        param_start = inc_config.get("date_param_start")
                        param_end = inc_config.get("date_param_end")
                        
                        filter_field = inc_config.get("date_filter_field")
                        filter_value = inc_config.get("date_filter_value")

                        if param_start:
                            end_date = datetime.now()
                            start_date = end_date - timedelta(days=days)
                            date_format = "%Y-%m-%d"
                            
                            if getattr(job_spec, 'params', None) is None: 
                                job_spec.params = {}
                                
                            # 1. Sobrescreve Data Inicial (Obrigatório)
                            job_spec.params[param_start] = start_date.strftime(date_format)
                            
                            # 2. Injeta Data Final (Opcional)
                            if param_end:
                                job_spec.params[param_end] = end_date.strftime(date_format)
                            
                            # 3. Injeta Filtro Extra (Opcional)
                            if filter_field and filter_value:
                                job_spec.params[filter_field] = filter_value
                                
                            log_msg_end = f" até {end_date.strftime(date_format)}" if param_end else " (sem data final)"
                            log_msg_extra = f" | Filtro: {filter_field}={filter_value}" if (filter_field and filter_value) else ""
                            
                            log.info(f"[{job_spec.name}] Carga incremental ativada: {start_date.strftime(date_format)}{log_msg_end}{log_msg_extra}")

                    elif is_full_load_mode:
                        log.info(f"[{job_spec.name}] Executando em modo Agendado (Params Estáticos).")

                    # --- C. EXECUÇÃO ---
                    effective_limit = limit if (preview or export) else 0
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

                    # Executa o ETL
                    result_tuple = datamat.run_etl_job(adapter, job_spec, mapping_spec)
                    stg_results.append(result_tuple)
                    
                    total_rows_pipeline += (result_tuple[1] + result_tuple[2])

                except AuthenticationError as auth_err:
                    log.critical(f"⛔ FALHA DE AUTENTICAÇÃO no job '{job_spec.name}': {auth_err}")
                    observer.notify_failure(tenant_id, f"AUTH: {job_spec.name}", auth_err)
                    raise 

                except (DataMatError, Exception) as e:
                    log.critical(f"Job '{job_spec.name}' falhou: {e}", exc_info=True)
                    datamat.log_etl_error(process_name=job_spec.name, message=str(e))
                    observer.notify_failure(tenant_id, job_spec.name, e)
                    raise Exception(f"Falha no Job '{job_spec.name}': {e}") from e
        else:
            log.info("Flag '--procs-only' ativa. Pulando a execução dos jobs de STG.")

        # 5. Execução de Procedures (DW)
        if procs_only or not (job_names or preview or export):
            if PROCS:
                log.info("Iniciando execução das procedures do DW...")
                for group in PROCS:
                    for proc_config in group:
                        # Executa e verifica sucesso
                        success = datamat.run_dw_procedure(proc_config)
                        if success:
                            executed_proc_names.append(proc_config['name']) # <--- ADICIONADO: Captura o nome
            else:
                log.info("Nenhuma procedure definida para execução.")
        else:
            log.info("Procedures não executadas devido aos parâmetros de execução (--jobs, --preview, --export).")
        
        # Log local resumido (passando a lista de nomes agora)
        datamat.log_summary(tenant_id, stg_results, executed_proc_names)

    except Exception as e:
        log.critical(f"Erro inesperado no pipeline do tenant '{tenant_id}': {e}", exc_info=True)
        return -1, {"error": str(e), "traceback": traceback.format_exc()}, []
    
    log.info(f"================ FINALIZANDO PIPELINE PARA O TENANT: {tenant_id} ================")
    
    return total_rows_pipeline, stg_results, executed_proc_names


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de pipelines de ETL do DataMat.")
    parser.add_argument("tenants", nargs='*', help="IDs dos tenants. Se vazio, executa todos.")
    parser.add_argument("-j", "--jobs", nargs='+', help="Nomes dos jobs específicos.")
    parser.add_argument("-p", "--preview", action="store_true", help="Modo de preview.")
    parser.add_argument("-e", "--export", action="store_true", help="Exporta dados para Excel.")
    parser.add_argument("-l", "--linhas", type=int, default=10, help="Limite de linhas.")
    parser.add_argument("-po","--procs-only", action="store_true", help="Executa apenas procedures.")
    args = parser.parse_args()
    
    tenants_to_run = args.tenants
    if not tenants_to_run:
        all_tenants_dirs = [d for d in (ROOT_DIR / "tenants").iterdir() if d.is_dir() and not d.name.startswith('_')]
        tenants_to_run = [t.name for t in all_tenants_dirs if (t / "config" / ".env").exists()]
        
        if tenants_to_run:
            log.info(f"Nenhum tenant especificado. Executando para: {tenants_to_run}")
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