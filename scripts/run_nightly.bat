@echo off
setlocal enableextensions enabledelayedexpansion

rem ====== CONFIGURAÇÃO DINÂMICA DE CAMINHOS ======
rem %~dp0 é o caminho deste arquivo .bat (ex: C:\Projetos\DataMat\scripts\)
set "SCRIPT_DIR=%~dp0"

rem Navega para a pasta pai (Raiz do Projeto)
pushd "%SCRIPT_DIR%.."
set "PROJECT_DIR=%CD%"
popd

set "VENV_DIR=%PROJECT_DIR%\.venv"
set "PY=%VENV_DIR%\Scripts\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"

rem ====== UTF-8 ======
chcp 65001 >NUL
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=UTF-8"

rem ====== TIMESTAMP ======
for /f %%t in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HHmmss\""') do set "TS=%%t"

rem ====== VALIDAÇÃO ======
if not exist "%PY%" (
  echo [ERRO CRITICO] Python nao encontrado em: %PY%
  echo Voce criou o ambiente virtual? (python -m venv .venv)
  pause
  exit /b 1
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem ====== LIMPEZA DE LOGS ANTIGOS (Preservada) ======
rem Tenta ler LOG_RETENTION_DAYS do .env, default 30
set "LOG_RETENTION_DAYS=30"
if exist "%PROJECT_DIR%\.env" (
    for /f "usebackq tokens=1,2 delims==" %%a in ("%PROJECT_DIR%\.env") do (
        if /i "%%a"=="LOG_RETENTION_DAYS" set "LOG_RETENTION_DAYS=%%b"
    )
)

echo [INFO] Limpando logs com mais de %LOG_RETENTION_DAYS% dias...
powershell -NoProfile -Command ^
  "Try { Get-ChildItem -Path '%LOG_DIR%' -Filter '*.log' -File | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-[int]('%LOG_RETENTION_DAYS%')) } | Remove-Item -Force -ErrorAction Stop } Catch { }"

rem ====== EXECUÇÃO ======
set "LOGFILE=%LOG_DIR%\nightly_batch_%TS%.log"
echo ==== Iniciando Carga Noturna em %TS% ==== >> "%LOGFILE%"

rem Entra na raiz para garantir que os imports do Python funcionem
cd /d "%PROJECT_DIR%"

rem Executa o Master Nightly
"%PY%" -X utf8 -m core.master_nightly >> "%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"

echo ==== Fim da Execucao: RC=%RC% ==== >> "%LOGFILE%"

rem Se der erro, nao segura a janela (para automacao), mas exibe o codigo
exit /b %RC%