from __future__ import annotations
import importlib
import os
import re
import time
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.datamat import DataMat
from core.adapters.file_adapter import FileSourceAdapter
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter


# =========================
# ====== UTIL .ENV ========
# =========================
def _load_env_file(env_path: Path, *, required: bool = False, override: bool = True) -> None:
    if not env_path.exists():
        if required:
            raise FileNotFoundError(f".env não encontrado: {env_path}")
        return
    try:
        from dotenv import load_dotenv  # type: ignore
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
    ps = _get_int("DB_POOL_SIZE")
    if ps is not None: pool_kwargs["pool_size"] = ps
    mo = _get_int("DB_MAX_OVERFLOW")
    if mo is not None: pool_kwargs["max_overflow"] = mo
    pr = _get_int("DB_POOL_RECYCLE")
    if pr is not None: pool_kwargs["pool_recycle"] = pr
    pt = _get_int("DB_POOL_TIMEOUT")
    if pt is not None: pool_kwargs["pool_timeout"] = pt

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
# ===== BUILD ADAPTER =====
# =========================
def build_adapter(job) -> object:
    jtype = getattr(job, "type", None)

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
            
        adapter = APISourceAdapter(
            final_endpoint,
            paging=getattr(job, "paging", None) or None,
            timeout=getattr(job, "timeout", None) or int(os.getenv("API_TIMEOUT", "30")),
            auth=getattr(job, "auth", None),
            params=getattr(job, "params", None),
            enrich_by_id=getattr(job, "enrich_by_id", False)
        )
        return adapter

    if jtype == "db":
        source_url = expand_placeholders(job.source_url)
        query = expand_placeholders(job.query)
        return DatabaseSourceAdapter(source_url, query, params=getattr(job, "params", None))

    raise ValueError(f"Tipo de job desconhecido: {jtype}")


# =========================
# ===== RUN A JOB =========
# =========================
def run_job(dm: DataMat, job, mappings, *, preview_only: bool = False, export_path: Optional[Path] = None) -> Tuple[str, int, int]:
    t_job0 = time.perf_counter()
    print(f"▶️  [{job.name}] Iniciando extração...")

    try:
        adapter = build_adapter(job)

        t0 = time.perf_counter()
        df: pd.DataFrame = adapter.extract()
        print(f"✅ [{job.name}] Extração concluída: {len(df)} linhas em {time.perf_counter()-t0:.2f}s")

        if export_path:
            raw_path = export_path.with_name(f"{export_path.stem}_raw{export_path.suffix}")
            print(f"📦 [MODO EXPORT] Salvando dados brutos em: {raw_path}")
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_excel(raw_path, index=False)

        spec = mappings.get(getattr(job, "map_id", None))
        
        if not spec and getattr(job, "map_id", None):
             raise DataMat.DataMatError(f"map_id '{job.map_id}' não foi encontrado no arquivo mappings.py")

        eff_columns = getattr(job, "columns", None) or (list(spec.src_to_tgt.keys()) if spec else None)
        eff_rename  = getattr(job, "rename_map", None) or (spec.src_to_tgt if spec else None)
        eff_req     = getattr(job, "required", None)  or (spec.required if spec else None)
        eff_keys    = getattr(job, "key_cols", None)  or (spec.key_cols if spec else None)
        eff_cmp     = getattr(job, "compare_cols", None) or (spec.compare_cols if spec else None)
        eff_rules   = getattr(job, "validation_rules", None) or (spec.validation_rules if spec else {})

        if isinstance(eff_keys, (str, bytes)): eff_keys = [eff_keys]
        if eff_cmp is not None and isinstance(eff_cmp, (str, bytes)): eff_cmp = [eff_cmp]

        print(f"🔧 [{job.name}] Preparando dataframe (rename/required/trim)...")
        t1 = time.perf_counter()
        df = dm.prepare_dataframe(
            df, columns=eff_columns, rename_map=eff_rename,
            required=eff_req, drop_extra=True, strip_strings=True)
        print(f"✅ [{job.name}] Dataframe preparado: {len(df)} linhas em {time.perf_counter()-t1:.2f}s")

        if eff_rules:
            print(f"🔎 [{job.name}] Validando qualidade dos dados...")
            try:
                schema = pa.DataFrameSchema(
                    columns={
                        col_name: pa.Column(**rules)
                        for col_name, rules in eff_rules.items()
                    },
                    strict=False,
                    coerce=True
                )
                schema.validate(df, lazy=True)
                print(f"✅ [{job.name}] Validação de dados concluída com sucesso.")
            except pa.errors.SchemaErrors as err:
                error_summary = err.failure_cases.to_markdown()
                detailed_message = f"Falha na validação de dados:\n{error_summary}"
                raise DataMat.DataMatError(detailed_message)

        if export_path:
            print(f"📦 [MODO EXPORT] Salvando dados mapeados em: {export_path}")
            export_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_excel(export_path, index=False)
            return job.name, 0, 0

        print(f"💰 [{job.name}] Normalizando colunas monetárias (heurística)...")
        t3 = time.perf_counter()
        def _normalize_money_col(s: pd.Series) -> pd.Series:
            if s.dtype.kind in ("i", "u", "f"): return s.round(2)
            txt = (s.astype(str).str.replace(r"\s", "", regex=True).str.replace("R$", "", regex=False)
                 .str.replace(".", "", regex=False, fixed=True).str.replace(",", ".", regex=False, fixed=True))
            return pd.to_numeric(txt, errors="coerce").round(2)
        money_candidates = [c for c in df.columns if any(tok in c.lower() for tok in ("valor", "preco", "preço", "custo", "total"))]
        for c in money_candidates: df[c] = _normalize_money_col(df[c])
        print(f"✅ [{job.name}] Normalização monetária concluída em {time.perf_counter()-t3:.2f}s")

        if eff_keys:
            print(f"🧹 [{job.name}] Removendo duplicatas por chave {eff_keys} ...")
            t4, before = time.perf_counter(), len(df)
            order_cols = [c for c in ["updated_at", "data_movimentacao", "data_emissao"] if c in df.columns]
            if order_cols: df = df.sort_values(order_cols)
            df = df.drop_duplicates(subset=eff_keys, keep="last").reset_index(drop=True)
            print(f"✅ [{job.name}] Dedup: {before}->{len(df)} em {time.perf_counter()-t4:.2f}s")
        
        if preview_only:
            print("\n" + "="*80)
            print(f"🔍 [MODO PREVIEW] Job: {job.name}")
            print(f"ℹ️  Total de linhas extraídas e tratadas: {len(df)}")
            print(f"ℹ️  Colunas disponíveis: {df.columns.tolist()}")
            print("-" * 80)
            if not df.empty:
                print("📋 Amostra dos dados (5 primeiras linhas):")
                print(df.head(5).to_markdown(index=False))
            else:
                print("📋 O DataFrame resultante está vazio.")
            print("="*80 + "\n")
            return job.name, 0, 0

        print(f"🚚 [{job.name}] Carregando no destino '{job.table}' ...")
        t5 = time.perf_counter()
        
        if eff_keys and dm._is_mysql():
            inserted, updated = dm.merge_into_mysql(
                df, job.table, key_cols=eff_keys, compare_cols=eff_cmp,
                schema=getattr(job, "schema", None), job_name=job.name)
        else:
            inserted, updated = dm.to_db(df, job.table, schema=getattr(job, "schema", None))

        print(f"🎉 [{job.name}] Carga concluída: {inserted} inseridos, {updated} atualizados em {time.perf_counter()-t5:.2f}s")
        print(f"⏱️  [{job.name}] Tempo total do job: {time.perf_counter()-t_job0:.2f}s")
        return job.name, inserted, updated

    except Exception as e:
        print(f"❌ [{job.name}] Falha na carga: {e.__class__.__name__} - {e}")
        dm.log_etl_error(process_name=job.name, message=str(e))
        raise

# =========================
# ===== RUN CLIENT ========
# =========================
def run_client(
    client_id: str, workers_per_client: int = 2, *,
    preview_job_name: Optional[str] = None, preview_only: bool = False,
    export_job_name: Optional[str] = None, export_path: Optional[str] = None
) -> Tuple[int, List]:
    load_project_and_tenant_env(client_id)

    jobs_mod = importlib.import_module(f"tenants.{client_id}.pipelines.jobs")
    mappings_mod = importlib.import_module(f"tenants.{client_id}.pipelines.mappings")

    JOBS = getattr(jobs_mod, "JOBS")
    PROCS = getattr(jobs_mod, "PROCS", [])
    MAPPINGS = getattr(mappings_mod, "MAPPINGS")

    is_debug_mode = preview_only or (export_path is not None)
    
    final_export_path = None
    if export_path:
        final_export_path = Path("tenants") / client_id / "data" / export_path

    if is_debug_mode:
        job_name = preview_job_name or export_job_name
        if not job_name: raise ValueError("O nome do job é obrigatório para o modo preview ou export (--job).")
        
        target_jobs = [job for job in JOBS if job.name == job_name]
        if not target_jobs:
            available_jobs = [j.name for j in JOBS]
            raise ValueError(f"Job '{job_name}' não encontrado. Jobs disponíveis: {available_jobs}")
        JOBS = target_jobs

    ingest_cfg = {
        "schema": os.getenv("INGEST_SCHEMA"), "if_exists": os.getenv("INGEST_IF_EXISTS", "append"),
        "chunksize": int(os.getenv("INGEST_CHUNKSIZE", "2000")), "method": os.getenv("INGEST_METHOD", "multi"),
        "collation": os.getenv("MYSQL_COLLATION") or "utf8mb4_unicode_ci",
    }
    workers_env = int(os.getenv("WORKERS_PER_CLIENT", workers_per_client))
    workers = min(max(1, workers_env), len(JOBS))
    dm_cache: Dict[str, DataMat] = {}

    def get_dm_for_job(job) -> DataMat:
        db_name_raw = getattr(job, "db_name", None)
        if not db_name_raw: raise RuntimeError(f"Job '{job.name}' sem db_name definido.")
        resolved = os.getenv(db_name_raw, db_name_raw)
        if resolved not in dm_cache:
            engine = make_engine_for_db(resolved)
            dm_cache[resolved] = DataMat(engine, ingest_cfg)
        return dm_cache[resolved]

    stg_results: List[Tuple[str, int, int]] = []
    
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_job, get_dm_for_job(job), job, MAPPINGS, preview_only=preview_only, export_path=final_export_path): job for job in JOBS}
        for fut in as_completed(futures):
            try:
                name, inserted, updated = fut.result()
                if not is_debug_mode:
                    print(f"📦 [STG] {name}: {inserted} inseridos, {updated} atualizados.")
                stg_results.append((name, inserted, updated))
            except Exception as e:
                job_failed = futures[fut]
                print(f"📦 [STG] FALHA no job: {job_failed.name} - {e.__class__.__name__} - {e}")
                stg_results.append((job_failed.name, -1, -1))

    if is_debug_mode:
        print("✅ Modo de depuração concluído.")
        return 0, []

    print(f"🔄 [{client_id}] Sincronizando objetos do DW (Views e Procedures)...")
    tenant_dw_path = Path("tenants") / client_id / "dw"
    if tenant_dw_path.is_dir():
        view_files = sorted(list((tenant_dw_path / "views").glob('**/*.sql')))
        proc_files = sorted(list((tenant_dw_path / "procedures").glob('**/*.sql')))
        sql_files = view_files + proc_files
        
        if sql_files:
            dw_db_name = os.getenv("DB_DW_NAME")
            if not dw_db_name: raise RuntimeError("DB_DW_NAME não está definido no .env para sincronizar os objetos do DW.")
            if dw_db_name not in dm_cache: dm_cache[dw_db_name] = DataMat(make_engine_for_db(dw_db_name), ingest_cfg)
            dm_dw = dm_cache[dw_db_name]

            for sql_file in sql_files:
                relative_path = sql_file.relative_to(tenant_dw_path)
                print(f"   -> Aplicando {relative_path}...")
                script_content = sql_file.read_text(encoding="utf-8")
                dm_dw.execute_sql_script(script_content)
            print(f"✅ [{client_id}] Objetos do DW sincronizados.")

    dw_inserted, dw_updated = 0, 0
    proc_group_num = 1
    for proc_group in PROCS:
        print(f"▶️  [DW] Executando GRUPO {proc_group_num} de procedures...")
        if not isinstance(proc_group, list): raise TypeError(f"A estrutura de 'PROCS' deve ser uma lista de listas. O grupo {proc_group_num} não é uma lista.")
        for proc_info in proc_group:
            proc_name, dm_dw = "desconhecida", None
            try:
                if isinstance(proc_info, dict):
                    dbn = os.getenv(proc_info.get("db_name", ""), "")
                    if not dbn: raise RuntimeError(f"PROCS: db_name inválido ou não resolvido no grupo {proc_group_num}.")
                    dm_dw = dm_cache.get(dbn) or DataMat(make_engine_for_db(dbn), ingest_cfg)
                    proc_sql_raw = proc_info["sql"].strip()
                    match = re.match(r"^\s*CALL\s+([\w\.]+)", proc_sql_raw, re.IGNORECASE)
                    if not match: raise ValueError(f"Comando de procedure inválido, esperado 'CALL nome_proc()': {proc_sql_raw}")
                    proc_name = match.group(1)
                    proc_sql_final = f"CALL {proc_name}(@p_inserted_rows, @p_updated_rows);"
                    print(f"   -> Executando procedure: {proc_name}...")
                    
                    with dm_dw.engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            conn.execute(text(proc_sql_final))
                            result = conn.execute(text("SELECT @p_inserted_rows, @p_updated_rows;")).fetchone()
                            trans.commit()
                            inserted, updated = (int(result[0]), int(result[1])) if result and result[0] is not None else (0, 0)
                        except Exception:
                            trans.rollback()
                            raise
                    print(f"   -> ✅ {proc_name}: {inserted} linhas inseridas, {updated} linhas atualizadas.")
                    dw_inserted += inserted; dw_updated += updated
            except Exception as e:
                print(f"   -> ❌ FALHA na procedure: {proc_name} - {e.__class__.__name__} - {e}")
                if dm_dw: dm_dw.log_etl_error(process_name=proc_name, message=str(e))
        print(f"✅ [DW] GRUPO {proc_group_num} concluído."); proc_group_num += 1

    stg_inserted = sum(i for _, i, _ in stg_results if i != -1)
    stg_updated = sum(u for _, _, u, in stg_results if u != -1)
    
    print("\n" + "="*50)
    print(f"📊 RESUMO FINAL DA CARGA PARA O CLIENTE: {client_id}")
    print("="*50)
    print(f"STG - Total Inserido:   {stg_inserted}")
    print(f"STG - Total Atualizado: {stg_updated}")
    print(f"DW  - Total Inserido:   {dw_inserted}")
    print(f"DW  - Total Atualizado: {dw_updated}")
    print("="*50 + "\n")

    total_geral = stg_inserted + stg_updated + dw_inserted + dw_updated
    return total_geral, stg_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador de ETL para clientes.")
    parser.add_argument("client_id", help="ID do cliente a ser processado (ex: HASHTAG).")
    parser.add_argument("--job", dest="job_name", help="Executa apenas um job específico em modo preview ou export.")
    parser.add_argument("--preview", action="store_true", help="Ativa o modo preview (requer --job).")
    parser.add_argument("--export", dest="export_path", help="Ativa o modo de exportação para um arquivo XLSX (requer --job).")
    args = parser.parse_args()
    
    if (args.preview or args.export_path) and not args.job_name:
        parser.error("--preview e --export requerem que --job seja especificado.")
    if args.preview and args.export_path:
        parser.error("Não é possível usar --preview e --export ao mesmo tempo.")

    load_project_and_tenant_env(client_id=None)
    run_client(
        client_id=args.client_id,
        preview_job_name=args.job_name,
        preview_only=args.preview,
        export_job_name=args.job_name,
        export_path=args.export_path
    )