# core/main.py
from __future__ import annotations

import importlib
import os
import re
import time
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from core.datamat import DataMat
from core.adapters.file_adapter import FileSourceAdapter
from core.adapters.api_adapter import APISourceAdapter
from core.adapters.db_adapter import DatabaseSourceAdapter


# =========================
# ====== UTIL .ENV ========
# =========================
def _load_env_file(env_path: Path, *, required: bool = False, override: bool = True) -> None:
    """
    Carrega variáveis de um arquivo .env para os.environ.
    - Se required=True e o arquivo não existir -> levanta erro.
    - Usa python-dotenv se disponível; caso contrário, faz um parse simples.
    """
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
    """
    Carrega primeiro o .env GLOBAL (raiz do projeto), depois o .env do CLIENTE.
    O do cliente sobrescreve o global quando houver conflito.
    """
    # 1) .env global (opcional)
    _load_env_file(Path(".env"), required=False, override=True)

    # 2) .env do cliente (se fornecido)
    if client_id:
        tenant_env = Path("tenants") / client_id / "config" / ".env"
        _load_env_file(tenant_env, required=True, override=True)


# =========================
# === PLACEHOLDER HELPER ===
# =========================
_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

def expand_placeholders(value: Any) -> Any:
    """Expande ${VAR} com valores de os.environ. Se não achar, mantém literal."""
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
    """
    Cria (ou reaproveita) um Engine para o banco informado.
    - Requer DB_URL no .env, com um {db} para inject do nome do banco.
      Ex.: DB_URL="mysql+pymysql://user:pass@host:3306/{db}?charset=utf8mb4"
    """
    if db_name in _engine_cache:
        return _engine_cache[db_name]

    tmpl = os.getenv("DB_URL")
    if not tmpl:
        raise RuntimeError("DB_URL não definido no .env (ex.: mysql+pymysql://user:pass@host:3306/{db}?charset=utf8mb4)")
    if "{db}" not in tmpl:
        raise RuntimeError("DB_URL não contém o placeholder {db}. Ex.: .../{db}?charset=utf8mb4")

    url = tmpl.format(db=db_name)

    # Pool/echo
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

    # connect_args a partir de DB_CONNECT_*
    connect_args: Dict[str, Any] = {}
    for k, v in os.environ.items():
        if k.startswith("DB_CONNECT_"):
            kk = k[len("DB_CONNECT_"):].lower()
            vv = _auto(v)
            connect_args[kk] = vv

    # mysql: garantir local_infile=1, se não desabilitado
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
        endpoint = expand_placeholders(job.endpoint)

        adapter = APISourceAdapter(
            endpoint,
            token_env=job.auth_env,
            paging=getattr(job, "paging", None) or None,
            timeout=getattr(job, "timeout", None) or int(os.getenv("API_TIMEOUT", "30")),
        )

        # PUSH-DOWN INCREMENTAL: se job trouxer date_field/from_date, configuramos no adapter
        if getattr(job, "date_field", None) and getattr(job, "from_date", None):
            adapter.configure_incremental(field=str(job.date_field), from_date=str(job.from_date))

        return adapter

    if jtype == "db":
        source_url = expand_placeholders(job.source_url)
        query = expand_placeholders(job.query)
        return DatabaseSourceAdapter(source_url, query, params=job.params)

    raise ValueError(f"Tipo de job desconhecido: {jtype}")


# =========================
# ===== INCREMENTAL =======
# =========================
def apply_incremental(df: pd.DataFrame, date_field: str | None, from_date: str | None) -> pd.DataFrame:
    """
    Filtro em memória (fallback). Mantido para fontes que NÃO suportam filtro push-down.
    Para API, o push-down já foi aplicado em build_adapter via configure_incremental().
    """
    if not date_field or not from_date or date_field not in df.columns:
        return df
    col = pd.to_datetime(df[date_field], errors="coerce", utc=False)
    cut = pd.to_datetime(from_date, utc=False)
    mask = col.notna() & (col >= cut)
    return df.loc[mask].reset_index(drop=True)


# =========================
# ===== RUN A JOB =========
# =========================
def run_job(dm: DataMat, job, mappings) -> Tuple[str, int]:
    t_job0 = time.perf_counter()
    print(f"▶️  [{job.name}] Iniciando extração...")

    adapter = build_adapter(job)

    t0 = time.perf_counter()
    df: pd.DataFrame = adapter.extract()
    print(f"✅ [{job.name}] Extração concluída: {len(df)} linhas em {time.perf_counter()-t0:.2f}s")

    # ---------- mapping ----------
    spec = mappings.get(getattr(job, "map_id", None))
    eff_columns = getattr(job, "columns", None) or (list(spec.src_to_tgt.keys()) if spec else None)
    eff_rename  = getattr(job, "rename_map", None) or (spec.src_to_tgt if spec else None)
    eff_req     = getattr(job, "required", None)  or (spec.required if spec else None)
    eff_keys    = getattr(job, "key_cols", None)  or (spec.key_cols if spec else None)
    eff_cmp     = getattr(job, "compare_cols", None) or (spec.compare_cols if spec else None)

    # aceitar string
    if isinstance(eff_keys, (str, bytes)):
        eff_keys = [eff_keys]
    if eff_cmp is not None and isinstance(eff_cmp, (str, bytes)):
        eff_cmp = [eff_cmp]

    # ---------- preparo ----------
    print(f"🔧 [{job.name}] Preparando dataframe (rename/required/trim)...")
    t1 = time.perf_counter()
    df = dm.prepare_dataframe(
        df,
        columns=eff_columns,
        rename_map=eff_rename,
        required=eff_req,
        drop_extra=True,
        strip_strings=True,
    )
    print(f"✅ [{job.name}] Dataframe preparado: {len(df)} linhas em {time.perf_counter()-t1:.2f}s")

    # ---------- incremental fallback (se necessário) ----------
    if getattr(job, "date_field", None) and getattr(job, "from_date", None):
        print(f"⏱️  [{job.name}] Aplicando filtro incremental em memória (fallback)...")
        t2 = time.perf_counter()
        before = len(df)
        df = apply_incremental(df, job.date_field, job.from_date)
        print(f"✅ [{job.name}] Incremental fallback: {before}->{len(df)} em {time.perf_counter()-t2:.2f}s")

    # ---------- normalização monetária ----------
    print(f"💰 [{job.name}] Normalizando colunas monetárias (heurística)...")
    t3 = time.perf_counter()
    def _normalize_money_col(s: pd.Series) -> pd.Series:
        if s.dtype.kind in ("i", "u", "f"):
            return s.round(2)
        txt = (
            s.astype(str)
             .str.replace(r"\s", "", regex=True)
             .str.replace("R$", "", regex=False)
             .str.replace(".", "", regex=False)   # milhares
             .str.replace(",", ".", regex=False)  # decimal
        )
        return pd.to_numeric(txt, errors="coerce").round(2)

    money_candidates = [c for c in df.columns if any(tok in c.lower() for tok in ("valor", "preco", "preço", "custo", "total"))]
    for c in money_candidates:
        df[c] = _normalize_money_col(df[c])
    print(f"✅ [{job.name}] Normalização monetária concluída em {time.perf_counter()-t3:.2f}s")

    # ---------- dedup ----------
    if eff_keys:
        print(f"🧹 [{job.name}] Removendo duplicatas por chave {eff_keys} ...")
        t4 = time.perf_counter()
        before = len(df)
        order_cols = [c for c in ["updated_at", "data_movimentacao", "data_emissao"] if c in df.columns]
        if order_cols:
            df = df.sort_values(order_cols)
        df = df.drop_duplicates(subset=eff_keys, keep="last").reset_index(drop=True)
        print(f"✅ [{job.name}] Dedup: {before}->{len(df)} em {time.perf_counter()-t4:.2f}s")

    # ---------- carga ----------
    print(f"🚚 [{job.name}] Carregando no destino '{job.table}' ...")
    t5 = time.perf_counter()
    try:
        if eff_keys and dm._is_mysql():
            inserted = dm.merge_into_mysql(
                df, job.table,
                key_cols=eff_keys,
                compare_cols=eff_cmp,
                schema=getattr(job, "schema", None),   # opcional
                job_name=job.name,
            )
        else:
            inserted = dm.to_db(df, job.table, schema=getattr(job, "schema", None))

        print(f"🎉 [{job.name}] Carga concluída: {inserted} linhas em {time.perf_counter()-t5:.2f}s")
        print(f"⏱️  [{job.name}] Tempo total do job: {time.perf_counter()-t_job0:.2f}s")
        return job.name, inserted

    except Exception as e:
        print(f"❌ [{job.name}] Falha na carga: {e.__class__.__name__}")
        dm.log.exception(
            "ETL FAILED | job=%s table=%s rows=%d key_cols=%s compare_cols=%s | %s",
            getattr(job, "name", "?"),
            getattr(job, "table", "?"),
            len(df) if isinstance(df, pd.DataFrame) else -1,
            eff_keys,
            eff_cmp,
            e.__class__.__name__,
        )
        raise


# =========================
# ===== RUN CLIENT ========
# =========================
def run_client(client_id: str, workers_per_client: int = 2) -> Tuple[int, List[Tuple[str, int]]]:
    """
    Executa TODOS os jobs do cliente em PARALELO e retorna total e lista de resultados.
    .env global (./.env) é carregado, depois o .env do cliente em tenants/<client_id>/config/.env.
    """
    # carrega .env global + do cliente (cliente sobrescreve)
    load_project_and_tenant_env(client_id)

    # importa jobs e mappings do cliente
    jobs_mod = importlib.import_module(f"tenants.{client_id}.pipelines.jobs")
    mappings_mod = importlib.import_module(f"tenants.{client_id}.pipelines.mappings")

    JOBS = getattr(jobs_mod, "JOBS")
    PROCS = getattr(jobs_mod, "PROCS", [])
    MAPPINGS = getattr(mappings_mod, "MAPPINGS")

    # ingest cfg do .env
    ingest_cfg = {
        "schema":    os.getenv("INGEST_SCHEMA") or None,  # pode estar vazio
        "if_exists": os.getenv("INGEST_IF_EXISTS", "append"),
        "chunksize": int(os.getenv("INGEST_CHUNKSIZE", "2000")),
        "method":    os.getenv("INGEST_METHOD", "multi"),
        "collation": os.getenv("MYSQL_COLLATION") or "utf8mb4_unicode_ci",
    }

    # definir nº de workers
    workers_env = int(os.getenv("WORKERS_PER_CLIENT", workers_per_client))
    workers = min(max(1, workers_env), len(JOBS))

    # cache de DataMat por banco
    dm_cache: Dict[str, DataMat] = {}

    def get_dm_for_job(job) -> DataMat:
        db_name_raw = getattr(job, "db_name", None)
        if not db_name_raw:
            raise RuntimeError(f"Job '{job.name}' sem db_name definido.")
        # resolve via .env do cliente (ex.: DB_STG_NAME=HASHTAG_STG)
        resolved = os.getenv(db_name_raw, db_name_raw)
        if resolved not in dm_cache:
            engine = make_engine_for_db(resolved)
            dm_cache[resolved] = DataMat(engine, ingest_cfg)
        return dm_cache[resolved]

    total = 0
    resultados: List[Tuple[str, int]] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for job in JOBS:
            dm = get_dm_for_job(job)
            futures.append(pool.submit(run_job, dm, job, MAPPINGS))

        for fut in as_completed(futures):
            name, n = fut.result()
            print(f"📦 [{client_id}] {name}: {n} linhas")
            resultados.append((name, n))
            total += n

    # Executar PROCS (se houver)
    for proc in PROCS:
        if isinstance(proc, dict):
            dbn = os.getenv(proc.get("db_name", ""), "")
            if not dbn:
                raise RuntimeError("PROCS: db_name inválido ou não resolvido.")
            dm = dm_cache.get(dbn) or DataMat(make_engine_for_db(dbn), ingest_cfg)
            dm.call_proc(proc["sql"])
        else:
            if not dm_cache:
                raise RuntimeError("Sem DataMat disponível para executar PROCS.")
            next(iter(dm_cache.values())).call_proc(proc)

    print(f"✅ [{client_id}] Total inserido: {total}")
    return total, resultados


# =========================
# ========= MAIN ==========
# =========================
if __name__ == "__main__":
    import sys
    # carrega apenas o .env global para descobrir CLIENT_ID (se definido lá)
    load_project_and_tenant_env(client_id=None)

    client = os.getenv("CLIENT_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not client:
        raise SystemExit("Informe o CLIENT_ID (env global) ou como argumento. Ex.: CLIENT_ID=HASHTAG python -m core.main")
    run_client(client)
