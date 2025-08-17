@echo off
setlocal enableextensions enabledelayedexpansion

rem ====== CONFIG ======
set "PROJECT_DIR=D:\DATAMAT\_PY"
set "VENV_DIR=%PROJECT_DIR%\.venv"
set "PY=%VENV_DIR%\Scripts\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"

rem ====== UTF-8 no shell e no Python ======
chcp 65001 >NUL
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=UTF-8"

rem ====== CARREGA LOG_RETENTION_DAYS DO .ENV ======
for /f "usebackq tokens=1,2 delims==" %%a in ("%PROJECT_DIR%\.env") do (
  if /i "%%a"=="LOG_RETENTION_DAYS" set "LOG_RETENTION_DAYS=%%b"
)

rem fallback default (30 dias)
if not defined LOG_RETENTION_DAYS set "LOG_RETENTION_DAYS=30"

rem ====== TIMESTAMP (locale-safe) ======
for /f %%t in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd_HHmmss\""') do set "TS=%%t"

rem ====== PREP ======
if not exist "%PROJECT_DIR%" (
  echo [ERRO] PROJECT_DIR nao existe: %PROJECT_DIR%
  exit /b 2
)
if not exist "%VENV_DIR%" (
  echo [ERRO] VENV_DIR nao existe: %PROJECT_DIR%
  exit /b 3
)
if not exist "%PY%" (
  echo [ERRO] Python do venv nao encontrado: %PY%
  exit /b 4
)
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem ====== LIMPEZA DE LOGS ANTIGOS ======
powershell -NoProfile -Command ^
  "Try { Get-ChildItem -Path '%LOG_DIR%' -Filter '*.log' -File | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-[int]('%LOG_RETENTION_DAYS%')) } | Remove-Item -Force -ErrorAction Stop } Catch { }"

set "LOGFILE=%LOG_DIR%\nightly_%TS%.log"

rem ====== EXECUCAO ======
echo ==== Iniciando core/master_nightly.py em %TS% ====>>"%LOGFILE%"
pushd "%PROJECT_DIR%"

"%PY%" -X utf8 -m core.master_nightly >>"%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"

popd
echo ==== Fim: RC=%RC% ====>>"%LOGFILE%"
exit /b %RC%
