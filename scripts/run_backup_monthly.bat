@echo off
setlocal

:: =======================================================
:: DATAMAT - MONTHLY BACKUP RUNNER
:: Executa backup e notifica via Telegram (Sem logs locais)
:: =======================================================

:: 1. Define Caminhos Relativos
set "SCRIPT_DIR=%~dp0"
:: Navega para a raiz do projeto (uma pasta acima de scripts)
pushd "%SCRIPT_DIR%.."
set "PROJECT_DIR=%CD%"
popd

:: 2. Configura Python do Virtual Env
set "VENV_DIR=%PROJECT_DIR%\.venv"
set "PY=%VENV_DIR%\Scripts\python.exe"

:: 3. Configura Encoding UTF-8 (Crucial para acentos no Python)
chcp 65001 >NUL
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=UTF-8"

:: 4. Validação e Execução
if exist "%PY%" (
    :: Executa o script. Não usamos ">>" pois o log é enviado pro Telegram.
    "%PY%" "%PROJECT_DIR%\scripts\full_server_backup.py"
) else (
    :: Se o Python não existir, não temos como avisar no Telegram.
    :: Nesse caso extremo, apenas aborta.
    exit /b 1
)

exit /b 0   