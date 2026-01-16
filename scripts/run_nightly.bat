@echo off
setlocal enableextensions enabledelayedexpansion

:: =======================================================
:: DATAMAT - NIGHTLY BATCH RUNNER
:: Wrapper simples para execução via Task Scheduler
:: =======================================================

:: 1. DEFINIÇÃO DE CAMINHOS ABSOLUTOS
set "SCRIPT_DIR=%~dp0"

:: Navega para a pasta pai (Raiz do Projeto)
pushd "%SCRIPT_DIR%.."
set "PROJECT_DIR=%CD%"
popd

:: Configurações do Ambiente
set "VENV_DIR=%PROJECT_DIR%\.venv"
set "PY=%VENV_DIR%\Scripts\python.exe"
set "LOG_DIR=%PROJECT_DIR%\nightly_logs"

:: 2. FORÇAR UTF-8 (Essencial para o Python não quebrar com acentos)
chcp 65001 >NUL
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=UTF-8"

:: 3. VALIDAÇÃO SIMPLES
if not exist "%PY%" (
  :: Se não achar o Python, não temos onde logar, então apenas sai com erro.
  exit /b 1
)

:: 4. LIMPEZA DE LOGS ANTIGOS (Manutenção)
:: Mantive apenas a limpeza para não acumular arquivos infinitamente.
:: Ele vai limpar os logs gerados pelo próprio Python na pasta nightly_logs.
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_RETENTION_DAYS=30"
powershell -NoProfile -Command "Try { Get-ChildItem -Path '%LOG_DIR%' -Filter '*.log' -File | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-[int]('%LOG_RETENTION_DAYS%')) } | Remove-Item -Force -ErrorAction SilentlyContinue } Catch { }"

:: 5. EXECUÇÃO
:: Garante que o diretório de trabalho seja a raiz do projeto
cd /d "%PROJECT_DIR%"

:: Executa o Master Nightly
:: Sem redirecionamento (>>). O Python gerencia seu próprio arquivo de log.
"%PY%" core\master_nightly.py

set "RC=%ERRORLEVEL%"
exit /b %RC%