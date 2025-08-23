# Orquestrador da madrugada com alertas via E-mail e Telegram
from __future__ import annotations

# ===== UTF-8 stdio (mantém emojis no Windows/Scheduler) =====
import sys
import os
import traceback
from typing import List, Tuple, Dict, Iterable
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

from core.main import run_client
from core.alerts.email import send_email
from core.alerts.telegram import send_telegram_text


def _force_utf8_stdio():
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
_force_utf8_stdio()

# ========= Util: resolução de caminhos e carga de .env =========
def _root_dir() -> Path:
    # este arquivo está em core/, então a raiz é um nível acima
    return Path(__file__).resolve().parents[1]


def _existing_files(files: Iterable[Path]) -> List[Path]:
    return [p for p in files if p.is_file()]


def _tenant_env_candidates(root: Path, cid: str) -> List[Path]:
    """
    Retorna os possíveis caminhos de .env para o tenant (na ordem de prioridade).
    Carregaremos todos os que existirem, na ordem, com override=True.
    """
    base = root / "tenants" / cid
    return [
        base / ".env",
        base / "config" / ".env",
        base / ".env.local",
    ]


def _load_root_env(root: Path) -> None:
    """
    Carrega o .env da raiz explicitamente (sem depender do cwd).
    Não faz override do que já estiver no ambiente (override=False),
    permitindo que o Scheduler/OS injete variáveis se necessário.
    """
    load_dotenv(dotenv_path=root / ".env", override=False, encoding="utf-8")


def _push_env_from_files(files: List[Path]) -> Dict[str, str | None]:
    """
    Carrega variáveis a partir de múltiplos .env (na ordem),
    aplicando override=True. Retorna um dicionário com os valores
    anteriores (para restauração), contendo chaves modificadas/adicionadas.
    """
    changed: Dict[str, str | None] = {}

    for env_file in files:
        kvs = dotenv_values(env_file, encoding="utf-8")
        for k, _ in kvs.items():
            if k is None:
                continue
            if k not in changed:
                changed[k] = os.environ.get(k)
        load_dotenv(dotenv_path=env_file, override=True, encoding="utf-8")

    return changed


def _pop_env(previous: Dict[str, str | None]) -> None:
    """
    Restaura o ambiente removendo/voltando chaves que foram alteradas.
    """
    for k, old_val in previous.items():
        if old_val is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = old_val


# ========= Leitura de parâmetros =========
def load_clients_from_env() -> List[str]:
    raw = os.getenv("NIGHTLY_CLIENTS", "").strip()
    if not raw:
        raise SystemExit("Defina NIGHTLY_CLIENTS no .env, ex: NIGHTLY_CLIENTS=HASHTAG,OUTROCLIENTE")
    # aceita vírgula ou ponto-e-vírgula
    if ";" in raw and "," not in raw:
        parts = raw.split(";")
    else:
        parts = raw.split(",")
    return [c.strip() for c in parts if c.strip()]


def workers_per_client() -> int:
    try:
        return int(os.getenv("WORKERS_PER_CLIENT", "2"))
    except Exception:
        return 2


# ========= Notificações =========
def _notify_failure(cid: str, e: Exception, tb: str) -> None:
    # Telegram (detalhado só em falha)
    tmsg = (
        f"🚨 *Falha na carga*\n"
        f"*Cliente:* `{cid}`\n"
        f"*Quando:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"*Erro:* `{e.__class__.__name__}`\n"
        f"```\n{str(e)}\n```\n"
        f"Traceback (resumo):\n"
        f"```\n{tb}\n```"
    )
    try:
        send_telegram_text(tmsg, parse_mode="Markdown")
    except Exception:
        pass

    # E-mail (apenas falha)
    subj = f"[ETL Nightly][{cid}] Falha na carga"
    body_txt = (
        f"Falha na carga do cliente {cid}\n\n"
        f"Quando: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Erro: {e.__class__.__name__}\n"
        f"Mensagem: {str(e)}\n\n"
        f"Traceback:\n{tb}\n"
    )
    body_html = f"""
    <h3>Falha na carga</h3>
    <p><b>Cliente:</b> {cid}<br>
       <b>Quando:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
       <b>Erro:</b> {e.__class__.__name__}</p>
    <pre style="white-space:pre-wrap">{str(e)}</pre>
    <h4>Traceback (resumo)</h4>
    <pre style="white-space:pre-wrap">{tb}</pre>
    """
    try:
        send_email(subj, body_txt, body_html)
    except Exception:
        pass


def _notify_client_result(cid: str, total: int, dur_s: float) -> None:
    """
    Notificação curta por cliente (para não 'spammar').
    Telegram apenas; e-mail fica para falhas e resumo final.
    """
    if total >= 0:
        msg = f"✅ *{cid}* — {total} linhas em {dur_s:.1f}s"
    else:
        msg = f"❌ *{cid}* — Falha (veja log)"
    try:
        send_telegram_text(msg, parse_mode="Markdown")
    except Exception:
        pass


def _notify_summary(resumo: List[Tuple[str, int]], dur_s: float) -> None:
    ok = sum(1 for _, t in resumo if t >= 0)
    falhas = sum(1 for _, t in resumo if t < 0)
    linhas = []
    for cid, tot in resumo:
        if tot >= 0:
            linhas.append(f"✅ {cid}: {tot} linhas")
        else:
            linhas.append(f"❌ {cid}: FALHA")

    # Telegram (resumo final)
    tmsg = (
        f"🌙 *Resumo carga madrugada*\n"
        f"*Quando:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"*Duração:* {dur_s:.1f}s\n"
        f"*Clientes OK:* {ok} | *Falhas:* {falhas}\n\n" +
        "\n".join(linhas)
    )
    try:
        send_telegram_text(tmsg, parse_mode="Markdown")
    except Exception:
        pass

    # E-mail (resumo final)
    subj = "[ETL Nightly] Resumo das cargas"
    body_txt = (
        f"Resumo da madrugada\n\n"
        f"Quando: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Duração: {dur_s:.1f}s\n"
        f"Clientes OK: {ok} | Falhas: {falhas}\n\n" +
        "\n".join(linhas) + "\n"
    )
    body_html = f"""
    <h3>Resumo carga madrugada</h3>
    <p><b>Quando:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
       <b>Duração:</b> {dur_s:.1f}s<br>
       <b>Clientes OK:</b> {ok} | <b>Falhas:</b> {falhas}</p>
    <ul>
      {''.join(f'<li>{ln}</li>' for ln in linhas)}
    </ul>
    """
    try:
        send_email(subj, body_txt, body_html)
    except Exception:
        pass


# ========= Execução principal =========
def run_all_clients() -> Tuple[List[Tuple[str, int]], float]:
    inicio = datetime.now()
    resumo: List[Tuple[str, int]] = []

    root = _root_dir()
    _load_root_env(root)  # garante .env da raiz

    CLIENTES = load_clients_from_env()
    W = workers_per_client()

    # Snapshot do ambiente base (após carregar raiz)
    base_snapshot = dict(os.environ)

    for cid in CLIENTES:
        print(f"\n=== Iniciando cliente: {cid} ===")

        # Carregar .env(s) do tenant com override, mantendo lista p/ restauração
        tenant_envs = _existing_files(_tenant_env_candidates(root, cid))
        if tenant_envs:
            print("🧩 [ENV] Carregando .env do tenant:", ", ".join(str(p) for p in tenant_envs))
            prev = _push_env_from_files(tenant_envs)
        else:
            print("ℹ️ [ENV] Nenhum .env específico encontrado para o tenant.")

        # --- execução por cliente (não interrompe os demais) ---
        cli_inicio = datetime.now()
        try:
            total, _detalhes = run_client(cid, workers_per_client=W)
            resumo.append((cid, total))
        except Exception as e:
            tb = traceback.format_exc(limit=12)
            resumo.append((cid, -1))
            _notify_failure(cid, e, tb)
        finally:
            cli_dur = (datetime.now() - cli_inicio).total_seconds()
            # notificação curta por cliente (sempre)
            last_total = resumo[-1][1]
            _notify_client_result(cid, last_total, cli_dur)

            # Restaurar ambiente ao estado base para o próximo cliente
            if tenant_envs:
                _pop_env(prev)
            os.environ.clear()
            os.environ.update(base_snapshot)

    dur = (datetime.now() - inicio).total_seconds()

    # resumo final
    _notify_summary(resumo, dur)

    print("\n===== RESUMO MADRUGADA =====")
    for cid, tot in resumo:
        print("-", f"{cid}: {'FALHA' if tot < 0 else f'{tot} linhas'}")
    print(f"Duração total: {dur:.1f}s")

    return resumo, dur


if __name__ == "__main__":
    run_all_clients()
