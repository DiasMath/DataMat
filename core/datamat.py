from __future__ import annotations
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql
from uuid import uuid4


class DataMat:
    class DataMatError(Exception):
        ...

    # ========= fábrica a partir do .env =========
    @staticmethod
    def _load_env(env_path: Union[str, Path]) -> None:
        env_path = Path(env_path)
        if not env_path.exists():
            return
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(env_path, override=True)
        except Exception:
            # parse simples
            for raw in env_path.read_text(encoding="utf-8").splitlines():
                s = raw.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

    @staticmethod
    def _get_pool_kwargs(prefix: str = "DB") -> Dict[str, Any]:
        def _iget(name: str) -> Optional[int]:
            v = os.getenv(name)
            try:
                return int(v) if v not in (None, "") else None
            except Exception:
                return None

        out: Dict[str, Any] = {}
        size = _iget(f"{prefix}_POOL_SIZE")
        if size is not None:
            out["pool_size"] = size
        mo = _iget(f"{prefix}_MAX_OVERFLOW")
        if mo is not None:
            out["max_overflow"] = mo
        rec = _iget(f"{prefix}_POOL_RECYCLE")
        if rec is not None:
            out["pool_recycle"] = rec
        to = _iget(f"{prefix}_POOL_TIMEOUT")
        if to is not None:
            out["pool_timeout"] = to
        return out

    @staticmethod
    def _connect_args(prefix: str = "DB") -> Dict[str, Any]:
        args: Dict[str, Any] = {}
        # copia quaisquer variáveis do tipo DB_CONNECT_*
        for k, v in os.environ.items():
            if k.startswith(f"{prefix}_CONNECT_"):
                kk = k[len(f"{prefix}_CONNECT_"):].lower()
                args[kk] = _auto(v)
        # defaults úteis para MySQL
        db_url = os.getenv("DB_URL", "")
        if db_url.lower().startswith("mysql"):
            args.setdefault("local_infile", 1)
        return args

    @classmethod
    def engine_from_env(cls, db_name: str, env_path: Union[str, Path]) -> Engine:
        """
        Cria um Engine lendo o .env:
          - DB_URL: ex. "mysql+pymysql://user:pass@host:3306/{db}?charset=utf8mb4"
          - Substitui {db} por `db_name`
          - Usa DB_ECHO e parâmetros de pool (DB_POOL_SIZE, etc.)
        """
        cls._load_env(env_path)

        url_tpl = os.getenv("DB_URL")
        if not url_tpl:
            raise cls.DataMatError("DB_URL ausente no .env (ex.: mysql+pymysql://user:pass@host:3306/{db}?charset=utf8mb4)")
        if "{db}" not in url_tpl:
            raise cls.DataMatError("DB_URL precisa conter o placeholder {db} para o nome do banco.")
        url = url_tpl.replace("{db}", db_name)

        echo = str(os.getenv("DB_ECHO", "0")).strip().lower() in ("1", "true", "on", "yes", "y")
        engine = create_engine(
            url,
            echo=echo,
            future=True,
            pool_pre_ping=True,
            connect_args=cls._connect_args("DB"),
            **cls._get_pool_kwargs("DB"),
        )
        return engine

    @classmethod
    def from_env(cls, db_name: str, env_path: Union[str, Path]) -> "DataMat":
        """
        Constrói um DataMat para o banco `db_name` com configs de ingestão do .env:
          - INGEST_SCHEMA (se vazio -> None)
          - INGEST_IF_EXISTS, INGEST_CHUNKSIZE, INGEST_METHOD
          - MYSQL_COLLATION
        """
        engine = cls.engine_from_env(db_name, env_path)

        schema_raw = os.getenv("INGEST_SCHEMA")
        ingest_schema = None if (schema_raw is None or schema_raw.strip() == "") else schema_raw.strip()

        cfg_ingest = {
            "schema":     ingest_schema,
            "if_exists":  os.getenv("INGEST_IF_EXISTS", "append"),
            "chunksize":  int(os.getenv("INGEST_CHUNKSIZE", "2000")),
            "method":     os.getenv("INGEST_METHOD", "multi"),
            "collation":  os.getenv("MYSQL_COLLATION") or "utf8mb4_unicode_ci",
        }
        return cls(engine, cfg_ingest)

    # ========= estado/ctor =========
    def __init__(self, engine: Engine, cfg_ingest: Dict[str, Any]) -> None:
        self.engine = engine
        self.cfg: Dict[str, Any] = {
            "schema":     cfg_ingest.get("schema"),
            "if_exists":  cfg_ingest.get("if_exists", "append"),
            "chunksize":  int(cfg_ingest.get("chunksize", 2000)),
            "method":     cfg_ingest.get("method", "multi"),
            "collation":  cfg_ingest.get("collation") or os.getenv("MYSQL_COLLATION") or "utf8mb4_unicode_ci",
        }
        # Credenciais cruas (quando possível) p/ PyMySQL (merge)
        self._mysql_conn_info: Dict[str, Any] = {}
        try:
            u = engine.url  # type: ignore
            self._mysql_conn_info = {
                "host": u.host, "port": u.port or 3306, "user": u.username,
                "password": u.password, "database": u.database, "charset": "utf8mb4",
            }
        except Exception:
            ...

        # logger minimalista (somente ERRO)
        self.log = logging.getLogger("datamat")
        if not self.log.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s - %(message)s"))
            self.log.addHandler(h)
        self.log.setLevel(logging.ERROR)

    # =========================
    # ======= PREPARO =========
    # =========================
    def prepare_dataframe(
        self, df: pd.DataFrame, *, columns: Optional[Iterable[str]] = None,
        rename_map: Optional[Dict[str, str]] = None, required: Optional[Iterable[str]] = None,
        drop_extra: bool = True, strip_strings: bool = True,
    ) -> pd.DataFrame:
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
                if pd.api.types.is_string_dtype(w[c]):
                    w[c] = w[c].astype("string").str.strip()
        return w

    # =========================
    # ======= HELPERS =========
    # =========================
    def _is_mysql(self) -> bool:
        try:
            name = (self.engine.dialect.name or "").lower()
            return name.startswith("mysql")
        except Exception:
            return False

    def _open_mysql_conn(self):
        info = self._mysql_conn_info or {}
        host = info.get("host"); port = info.get("port", 3306)
        user = info.get("user"); pwd = info.get("password")
        db   = info.get("database"); chs = info.get("charset", "utf8mb4")
        if not (host and user and pwd and db):
            raise self.DataMatError("Credenciais MySQL incompletas para PyMySQL.")
        return pymysql.connect(
            host=host, port=int(port), user=user, password=pwd,
            database=db, charset=chs, local_infile=True, autocommit=False,
        )

    @staticmethod
    def _mysql_type_for_series(s: pd.Series) -> str:
        from pandas.api import types as t
        if t.is_integer_dtype(s): return "BIGINT"
        if t.is_float_dtype(s):   return "DOUBLE"
        if t.is_bool_dtype(s):    return "TINYINT(1)"
        if t.is_datetime64_any_dtype(s): return "DATETIME"
        return "TEXT"

    @staticmethod
    def _is_str_dtype(series: pd.Series) -> bool:
        from pandas.api import types as t
        return t.is_string_dtype(series) or t.is_object_dtype(series)

    # ----- tipos/schema MySQL -----
    def _mysql_parse_type(self, type_str: str) -> dict:
        t = (type_str or "").strip().lower()
        m = re.match(r"(\w+)(?:\((\d+)(?:,(\d+))?\))?", t)
        if not m:
            return {"base": t, "precision": None, "scale": None}
        base = m.group(1)
        prec = int(m.group(2)) if m.group(2) else None
        scale = int(m.group(3)) if m.group(3) else None
        return {"base": base, "precision": prec, "scale": scale}

    def _mysql_column_types(self, cur, qualified_table: str) -> dict[str, dict]:
        cur.execute(f"SHOW COLUMNS FROM {qualified_table}")
        types: dict[str, dict] = {}
        for row in cur.fetchall():  # (Field, Type, Null, Key, Default, Extra)
            types[row[0]] = self._mysql_parse_type(row[1])
        return types

    def _coerce_df_to_mysql_schema(self, df: pd.DataFrame, coltypes: dict, *, job_name: str, table_name: str) -> pd.DataFrame:
        w = df.copy()
        for c in w.columns:
            info = coltypes.get(c)
            if not info:
                continue
            base = (info.get("base") or "").lower()
            if base == "decimal":
                prec = info.get("precision") or 18
                scale = info.get("scale") or 2
                s = w[c]
                if s.dtype.kind not in ("i", "u", "f"):
                    txt = (s.astype(str)
                           .str.replace(r"\s", "", regex=True)
                           .str.replace("R$", "", regex=False)
                           .str.replace(".", "", regex=False)
                           .str.replace(",", ".", regex=False))
                    s = pd.to_numeric(txt, errors="coerce")
                s = s.round(scale)
                max_abs = (10 ** (prec - scale)) - (10 ** (-scale))
                too_big = s.notna() & (s.abs() > max_abs)
                if too_big.any():
                    examples = s[too_big].head(5).tolist()
                    raise self.DataMatError(
                        f"Valor fora do range DECIMAL({prec},{scale}) em '{c}' | job={job_name} table={table_name} "
                        f"| exemplos={examples}."
                    )
                w[c] = s
            elif base in ("bigint", "int", "integer", "smallint", "tinyint"):
                w[c] = pd.to_numeric(w[c], errors="coerce").astype("Int64")
            elif base in ("double", "float", "real"):
                w[c] = pd.to_numeric(w[c], errors="coerce")
        return w

    def _mysql_target_columns(self, cur, qualified_table: str) -> list[str]:
        cur.execute(f"SHOW COLUMNS FROM {qualified_table}")
        return [row[0] for row in cur.fetchall()]

    def _mysql_has_unique_covering_index(self, cur, qualified_table: str, key_cols: list[str]) -> bool:
        cur.execute(f"SHOW INDEX FROM {qualified_table}")
        rows = cur.fetchall()
        uniques: dict[str, list[str]] = {}
        for r in rows:
            non_unique = r[1]
            key_name   = r[2]
            col_name   = r[4]
            if non_unique == 0:
                uniques.setdefault(key_name, []).append(col_name)
        key_set = set(key_cols)
        return any(key_set.issubset(set(cols)) for cols in uniques.values())

    # =========================
    # ======= ESCRITA =========
    # =========================
    def merge_into_mysql(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        key_cols: Iterable[str],
        compare_cols: Optional[Iterable[str]] = None,
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        job_name: Optional[str] = None,
    ) -> int:
        if df.empty:
            return 0
        if not self._is_mysql():
            raise self.DataMatError("merge_into_mysql requer dialeto MySQL.")

        # tratar '' como None
        eff_sc = schema if schema not in ("", None) else (self.cfg["schema"] or None)
        eff_cs = int(chunksize or self.cfg["chunksize"])
        coll   = self.cfg["collation"]

        if isinstance(key_cols, (str, bytes)): key_cols = [key_cols]
        if compare_cols is not None and isinstance(compare_cols, (str, bytes)): compare_cols = [compare_cols]
        key_cols = list(key_cols or [])
        if not key_cols:
            raise self.DataMatError("key_cols é obrigatório para MERGE.")
        miss = [c for c in key_cols if c not in df.columns]
        if miss:
            raise self.DataMatError(f"Chaves ausentes no DataFrame (job={job_name} table={table_name}): {miss}")

        if compare_cols is None:
            compare_cols = [c for c in df.columns if c not in key_cols]
        else:
            compare_cols = [c for c in compare_cols if c not in key_cols]

        tgt_tbl = f"`{eff_sc}`.`{table_name}`" if eff_sc else f"`{table_name}`"
        tmp_tbl_name = f"tmp__{table_name}__{uuid4().hex[:8]}"
        tmp_tbl = f"`{tmp_tbl_name}`"

        conn = self._open_mysql_conn()
        try:
            with conn.cursor() as cur:
                try:
                    target_cols = self._mysql_target_columns(cur, tgt_tbl)
                except Exception as eschema:
                    self.log.error("Erro ao listar colunas do alvo | job=%s table=%s | %s", job_name or "-", tgt_tbl, eschema)
                    raise

                missing_in_target = [c for c in df.columns if c not in target_cols]
                if missing_in_target:
                    msg = (f"Tabela alvo não possui colunas do DataFrame | job={job_name} table={tgt_tbl} "
                           f"| missing_in_target={missing_in_target}")
                    self.log.error(msg)
                    raise self.DataMatError(msg)

                col_types = self._mysql_column_types(cur, tgt_tbl)
                df = self._coerce_df_to_mysql_schema(df, col_types, job_name=job_name or "-", table_name=tgt_tbl)

                all_cols = list(df.columns)
                cols_csv = ", ".join(f"`{c}`" for c in all_cols)
                src_cols_csv = ", ".join(f"src.`{c}`" for c in all_cols)

                def _collate_expr(tbl_alias: str, col: str) -> str:
                    if self._is_str_dtype(df[col]):
                        return f"{tbl_alias}.`{col}` COLLATE {coll}"
                    return f"{tbl_alias}.`{col}`"

                on_expr = " AND ".join(f"{_collate_expr('tgt', c)} = {_collate_expr('src', c)}" for c in key_cols) or "1=1"
                def diff_col(c: str) -> str:
                    return f"NOT ({_collate_expr('tgt', c)} <=> {_collate_expr('src', c)})"
                change_pred = " OR ".join(diff_col(c) for c in compare_cols) if compare_cols else "0"
                upd_set = ", ".join([f"tgt.`{c}` = src.`{c}`" for c in compare_cols]) if compare_cols else ""

                def _tmp_col_def(col: str) -> str:
                    info = col_types.get(col)
                    if info and info.get("base") == "decimal" and info.get("precision") and info.get("scale") is not None:
                        return f"`{col}` DECIMAL({info['precision']},{info['scale']})"
                    return f"`{col}` {self._mysql_type_for_series(df[col])}"

                cols_def = ", ".join(_tmp_col_def(c) for c in all_cols)
                cur.execute(
                    f"CREATE TEMPORARY TABLE {tmp_tbl} ({cols_def}) "
                    f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE={coll};"
                )

                placeholders = ", ".join(["%s"] * len(all_cols))
                ins_stg_sql = f"INSERT INTO {tmp_tbl} ({cols_csv}) VALUES ({placeholders})"
                rows_iter = (tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False, name=None))

                batch = []; staged = 0
                for row in rows_iter:
                    batch.append(row)
                    if len(batch) >= eff_cs:
                        cur.executemany(ins_stg_sql, batch)
                        staged += len(batch); batch = []
                if batch:
                    cur.executemany(ins_stg_sql, batch); staged += len(batch)

                first_key = key_cols[0]
                cur.execute(f"""
                    SELECT COUNT(*) FROM {tmp_tbl} src
                    LEFT JOIN {tgt_tbl} tgt ON {on_expr}
                    WHERE tgt.`{first_key}` IS NULL
                """)
                pre_ins = int(cur.fetchone()[0])

                cur.execute(f"""
                    SELECT COUNT(*) FROM {tmp_tbl} src
                    JOIN {tgt_tbl} tgt ON {on_expr}
                    WHERE {change_pred}
                """)
                pre_upd = int(cur.fetchone()[0])

                if pre_ins > 0:
                    cur.execute(f"""
                        INSERT INTO {tgt_tbl} ({cols_csv})
                        SELECT {src_cols_csv}
                        FROM {tmp_tbl} src
                        LEFT JOIN {tgt_tbl} tgt ON {on_expr}
                        WHERE tgt.`{first_key}` IS NULL
                    """)

                if pre_upd > 0 and upd_set:
                    cur.execute(f"""
                        UPDATE {tgt_tbl} AS tgt
                        JOIN  {tmp_tbl} AS src ON {on_expr}
                        SET {upd_set}
                        WHERE {change_pred}
                    """)

                try:
                    cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_tbl};")
                except Exception:
                    ...

            conn.commit()
            return pre_ins + pre_upd

        except Exception:
            try: conn.rollback()
            except Exception: ...
            raise
        finally:
            try: conn.close()
            except Exception: ...

    def to_db(
        self, df: pd.DataFrame, table_name: str, * ,
        if_exists: Optional[str] = None, chunksize: Optional[int] = None,
        dtype_sql: Optional[Dict[str, Any]] = None, schema: Optional[str] = None,
    ) -> int:
        if df.empty:
            return 0

        eff_if = if_exists or self.cfg["if_exists"]
        eff_cs = chunksize or self.cfg["chunksize"]
        # tratar '' como None
        eff_sc = schema if schema not in ("", None) else (self.cfg["schema"] or None)

        try:
            df.to_sql(
                table_name,
                con=self.engine,
                schema=eff_sc,
                if_exists=eff_if,
                index=False,
                chunksize=eff_cs,
                method=self.cfg["method"],
                dtype=dtype_sql
            )
            return int(len(df))
        except SQLAlchemyError:
            raise

    # =========================
    # ======= PROCS SQL =======
    # =========================
    def call_proc(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql(sql)


# -------- utils locais --------
def _auto(x: str) -> Any:
    if x is None:
        return None
    s = str(x).strip()
    if s.lower() in ("1", "true", "t", "yes", "y", "on"): return True
    if s.lower() in ("0", "false", "f", "no", "n", "off"): return False
    try:
        return int(s)
    except Exception:
        try:
            return float(s)
        except Exception:
            return s
