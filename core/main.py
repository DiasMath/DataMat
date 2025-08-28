from __future__ import annotations
import importlib
import os
import re
import time
import logging
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

import pandas as pd
import pandera as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.datamat import DataMat
from core.adapters.file_adapter import FileSourceAdapter
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter

# Configuração de log para exibir apenas a mensagem, conforme solicitado.
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
log = logging.getLogger("DataMatOrchestrator")


# =========================
# ====== UTIL .ENV ========
# =========================
def _load_env_file(env_path: Path, *, required: bool = False, override: bool = True) -> None:
    if not env_path.exists():
        if required:
            raise FileNotFoundError(f".env não encontrado: {env_path}")
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=override)
    except Exception:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")


def load_project_and_tenant_env(client_id: Optional[str]) -> None:
    _load_env_file(Path(".env"), required=False, override=True)
    if client_id:
        tenant_env = Path("tenants") / client_id / "config" / ".env"
        _load_env_file(tenant_env, required=True, override=True)


# =========================
# === PLACEHOLDER HELPER ===
# =========================
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

def expand_placeholders(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    def repl(m: re.Match) -> str:
        var = m.group(1)
        return os.getenv(var, m.group(0))
    return _VAR_PATTERN.sub(repl, value)

def _auto(x: str):
    s = str(x).strip()
    low = s.lower()
    if low in ("1","true","t","yes","y","on"): return True
    if low in ("0","false","f","no","n","off"): return False
    try:
        return int(s)
    except Exception:
        try:
            return float(s)
        except Exception:
            return s

# =========================
# === ENGINE FACTORY ======
# =========================
_engine_cache: Dict[str, Engine] = {}

def make_engine_for_db(db_name: str) -> Engine:
    if db_name in _engine_cache:
        return _engine_cache[db_name]

    tmpl = os.getenv("DB_URL")
    if not tmpl:
        raise RuntimeError("DB_URL não definido no .env (ex.: mysql+pymysql://user:pass@host:3306/{db}?charset=utf8mb4)")
    if "{db}" not in tmpl:
        raise RuntimeError("DB_URL não contém o placeholder {db}. Ex.: .../{db}?charset=utf8mb4")

    url = tmpl.format(db=db_name)

    def _get_int(name: str, default: Optional[int] = None) -> Optional[int]:
        v = os.getenv(name)
        try:
            return int(v) if v not in (None, "") else default
        except Exception:
            return default

    echo = os.getenv("DB_ECHO", "0").strip().lower() in ("1", "true", "t", "yes", "y", "on")

    pool_kwargs: Dict[str, Any] = {}
    if (ps := _get_int("DB_POOL_SIZE")) is not None: pool_kwargs["pool_size"] = ps
    if (mo := _get_int("DB_MAX_OVERFLOW")) is not None: pool_kwargs["max_overflow"] = mo
    if (pr := _get_int("DB_POOL_RECYCLE")) is not None: pool_kwargs["pool_recycle"] = pr
    if (pt := _get_int("DB_POOL_TIMEOUT")) is not None: pool_kwargs["pool_timeout"] = pt

    connect_args: Dict[str, Any] = {}
    for k, v in os.environ.items():
        if k.startswith("DB_CONNECT_"):
            kk = k[len("DB_CONNECT_"):].lower()
            connect_args[kk] = _auto(v)

    if url.startswith("mysql"):
        connect_args.setdefault("local_infile", True)

    engine = create_engine(
        url,
        echo=echo,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
        **pool_kwargs,
    )
    _engine_cache[db_name] = engine
    return engine


# =========================
# ===== BUILD ADAPTER =====
# =========================
def build_adapter(job: Any, *, row_limit: Optional[int] = None) -> Any:
    jtype = getattr(job, "type", None)
    job_name = getattr(job, "name", "desconhecido")
    log.info(f"[{job_name}] Construindo adapter do tipo '{jtype}'...")

    if jtype == "file":
        file_path = expand_placeholders(job.file)
        return FileSourceAdapter(file_path, sheet=job.sheet, header=job.header)

    if jtype == "api":
        endpoint_path = expand_placeholders(job.endpoint)
        
        if endpoint_path.lower().startswith(("http://", "https://")):
            final_endpoint = endpoint_path
        else:
            base_url = os.getenv("API_BASE_URL", "").rstrip('/')
            if not base_url:
                raise RuntimeError(
                    f"Job '{job.name}' usa endpoint relativo '{endpoint_path}', "
                    "mas API_BASE_URL não está definida no .env."
                )
            final_endpoint = f"{base_url}/{endpoint_path.lstrip('/')}"
            
        return APISourceAdapter(
            final_endpoint,
            paging=getattr(job, "paging", None),
            auth=getattr(job, "auth", None),
            params=getattr(job, "params", None),
            enrich_by_id=getattr(job, "enrich_by_id", False),
            enrichment_strategy=getattr(job, "enrichment_strategy", 'concurrent'),
            row_limit=row_limit,
            data_path=getattr(job, "data_path", None),
            requests_per_minute=getattr(job, "requests_per_minute", None),
            enrichment_requests_per_minute=getattr(job, "enrichment_requests_per_minute", None)
        )

    if jtype == "db":
        source_url = expand_placeholders(job.source_url)
        query = expand_placeholders(job.query)
        return DatabaseSourceAdapter(source_url, query, params=getattr(job, "params", None))

    raise ValueError(f"Tipo de job desconhecido: {jtype}")


# =========================
# ===== RUN A JOB =========
# =========================
def run_job(dm: DataMat, job: Any, mapping_spec: Optional[Any], *, preview_only: bool = False, export_path: Optional[Path] = None, export_rows: Optional[int] = None) -> Tuple[str, int, int]:
    """Prepara os objetos e delega a execução para o DataMat ou para o modo de depuração."""
    adapter = build_adapter(job, row_limit=export_rows if (preview_only or export_path) else None)

    if preview_only or export_path:
        log.info(f"[{job.name}] Executando em modo de depuração (preview/export)...")
        df = adapter.extract()
        
        # Opcional: Adicionar lógica de preparação aqui se o preview/export precisar de dados limpos
        
        if export_path:
            log.info(f"📦 [{job.name}] [MODO EXPORT] Salvando {len(df)} linhas em: {export_path}")
            export_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_excel(export_path, index=False)
        
        if preview_only:
            print("\n" + "="*80)
            print(f"🔍 [MODO PREVIEW] Job: {job.name}")
            print(f"ℹ️  Total de linhas extraídas: {len(df)}")
            print(f"📋 Amostra dos dados:\n{df.head(export_rows or 5).to_markdown(index=False)}")
            print("="*80 + "\n")
            
        return job.name, 0, 0
    
    # Delega a execução principal para a classe DataMat
    return dm.run_etl_job(adapter, job, mapping_spec)


# =========================
# ===== RUN CLIENT ========
# =========================
def run_client(
    client_id: str, workers_per_client: int = 2, *,
    job_name_filter: Optional[str] = None,
    preview_only: bool = False,
    export_path: Optional[str] = None,
    export_rows: Optional[int] = None
) -> Tuple[int, List]:
    log.info(f"Iniciando execução para o cliente: {client_id}.")
    load_project_and_tenant_env(client_id)

    jobs_mod = importlib.import_module(f"tenants.{client_id}.pipelines.jobs")
    mappings_mod = importlib.import_module(f"tenants.{client_id}.pipelines.mappings")

    JOBS = getattr(jobs_mod, "JOBS")
    PROCS = getattr(jobs_mod, "PROCS", [])
    MAPPINGS = getattr(mappings_mod, "MAPPINGS")

    if job_name_filter:
        JOBS = [j for j in JOBS if j.name == job_name_filter]
        if not JOBS:
            raise ValueError(f"Job '{job_name_filter}' não encontrado.")

    final_export_path = Path("tenants") / client_id / "data" / export_path if export_path else None

    ingest_cfg = {
        "schema": os.getenv("INGEST_SCHEMA"),
        "if_exists": os.getenv("INGEST_IF_EXISTS", "append"),
        "chunksize": int(os.getenv("INGEST_CHUNKSIZE", "2000")),
        "method": os.getenv("INGEST_METHOD", "multi")
    }
    dm_cache: Dict[str, DataMat] = {}

    def get_dm_for_db(job: Any) -> DataMat:
        db_name = os.getenv(job.db_name, job.db_name)
        if db_name not in dm_cache:
            dm_cache[db_name] = DataMat(make_engine_for_db(db_name), ingest_cfg)
        return dm_cache[db_name]

    stg_results: List[Tuple[str, int, int]] = []
    
    with ThreadPoolExecutor(max_workers=workers_per_client) as pool:
        future_to_job = {
            pool.submit(run_job, get_dm_for_db(job), job, MAPPINGS.get(job.map_id),
                        preview_only=preview_only, export_path=final_export_path, export_rows=export_rows): job
            for job in JOBS
        }

        for fut in as_completed(future_to_job):
            job_failed = future_to_job[fut]
            try:
                name, inserted, updated = fut.result()
                if not (preview_only or export_path):
                    log.info(f"📦 [STG] Job '{name}': {inserted} inseridos, {updated} atualizados.")
                stg_results.append((name, inserted, updated))
            except Exception as e:
                log.error(f"📦 [STG] FALHA CRÍTICA no job '{job_failed.name}': {e}", exc_info=True)
                stg_results.append((job_failed.name, -1, -1))

    if preview_only or export_path or job_name_filter:
        log.info("✅ Execução em modo de depuração/filtro concluída.")
        return 0, []

    log.info(f"🔄 [{client_id}] Sincronizando objetos do DW (Views e Procedures)...")
    tenant_dw_path = Path("tenants") / client_id / "dw"
    if tenant_dw_path.is_dir():
        sql_files = sorted(list(tenant_dw_path.glob('**/*.sql')))
        
        if sql_files:
            dw_db_name = os.getenv("DB_DW_NAME")
            if not dw_db_name: raise RuntimeError("DB_DW_NAME não definido no .env")
            dm_dw = dm_cache.get(dw_db_name) or DataMat(make_engine_for_db(dw_db_name), ingest_cfg)

            for sql_file in sql_files:
                log.info(f"   -> Aplicando {sql_file.relative_to(tenant_dw_path)}...")
                dm_dw.execute_sql_script(sql_file.read_text(encoding="utf-8"))
            log.info(f"✅ [{client_id}] Objetos do DW sincronizados.")

    dw_inserted, dw_updated = 0, 0
    for i, proc_group in enumerate(PROCS):
        log.info(f"▶️  [DW] Executando GRUPO {i+1} de procedures...")
        for proc_info in proc_group:
            proc_name = "desconhecida"
            try:
                dbn = os.getenv(proc_info.get("db_name", ""), "")
                dm_dw = dm_cache.get(dbn) or DataMat(make_engine_for_db(dbn), ingest_cfg)
                proc_name = proc_info["sql"]
                log.info(f"   -> Executando procedure: {proc_name}...")
                
                with dm_dw.engine.connect() as conn:
                    with conn.begin():
                        conn.execute(text(f"{proc_name}(@p_inserted_rows, @p_updated_rows);"))
                        result = conn.execute(text("SELECT @p_inserted_rows, @p_updated_rows;")).fetchone()
                        inserted, updated = (int(result[0]), int(result[1])) if result and result[0] is not None else (0, 0)
                
                log.info(f"   -> ✅ {proc_name}: {inserted} linhas inseridas, {updated} linhas atualizadas.")
                dw_inserted += inserted
                dw_updated += updated
            except Exception as e:
                log.error(f"   -> ❌ FALHA na procedure: {proc_name} - {e}", exc_info=True)
                if 'dm_dw' in locals(): dm_dw.log_etl_error(process_name=proc_name, message=str(e))

    stg_inserted = sum(i for _, i, _ in stg_results if i != -1)
    stg_updated = sum(u for _, _, u in stg_results if u != -1)
    
    log.info("\n" + "="*50)
    log.info(f"📊 RESUMO FINAL DA CARGA PARA O CLIENTE: {client_id}")
    log.info("="*50)
    log.info(f"STG - Total Inserido:   {stg_inserted}")
    log.info(f"STG - Total Atualizado: {stg_updated}")
    log.info(f"DW  - Total Inserido:   {dw_inserted}")
    log.info(f"DW  - Total Atualizado: {dw_updated}")
    log.info("="*50 + "\n")

    total_geral = stg_inserted + stg_updated + dw_inserted + dw_updated
    return total_geral, stg_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador de ETL para clientes.")
    parser.add_argument("client_id", help="ID do cliente a ser processado.")
    parser.add_argument("--job", dest="job_name", help="Executa apenas um job específico.")
    parser.add_argument("--preview", action="store_true", help="Ativa o modo preview (requer --job).")
    parser.add_argument("--export", dest="export_path", help="Ativa o modo de exportação para um arquivo XLSX (requer --job).")
    parser.add_argument("--rows", dest="export_rows", type=int, help="Limita o número de linhas para --preview e --export.")
    
    args = parser.parse_args()
    
    if (args.preview or args.export_path) and not args.job_name:
        parser.error("--preview e --export requerem que --job seja especificado.")

    load_project_and_tenant_env(client_id=None) # Carrega .env global
    run_client(
        client_id=args.client_id,
        job_name_filter=args.job_name,
        preview_only=args.preview,
        export_path=args.export_path,
        export_rows=args.export_rows
    )