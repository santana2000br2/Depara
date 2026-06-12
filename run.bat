@echo off
chcp 65001 > nul
title Portal DePara - Flask (DEBUG MODE)

echo ========================================
echo    INICIANDO PORTAL DEPARA - FLASK
echo ========================================
echo.
echo MODO DEBUG: Todos os logs serao exibidos no console
echo.

REM Pasta do projeto = pasta deste .bat (funciona em qualquer disco/pasta)
set "PROJETO=%~dp0"

REM Verificar se o ambiente virtual existe, se nao, criar
if not exist "%PROJETO%venv" (
    echo Criando ambiente virtual...
    python -m venv "%PROJETO%venv"
)

REM Python do venv DESTE projeto (evita usar outro venv ja ativo no PATH)
set "PYEXE=%PROJETO%venv\Scripts\python.exe"
if not exist "%PYEXE%" (
    echo ERRO: Python do venv nao encontrado: %PYEXE%
    pause
    exit /b 1
)

echo Ativando ambiente virtual deste projeto...
call "%PROJETO%venv\Scripts\activate.bat"
set "PATH=%PROJETO%venv\Scripts;%PATH%"
set "VIRTUAL_ENV=%PROJETO%venv"

echo Atualizando pip, setuptools e wheel...
"%PYEXE%" -m pip install --upgrade pip setuptools wheel

echo Verificando dependencias...
"%PYEXE%" -m pip install -r "%PROJETO%requirements.txt"
if errorlevel 1 (
    echo.
    echo ERRO: Nao foi possivel instalar as dependencias. Corrija o erro acima e tente de novo.
    pause
    exit /b 1
)

if not exist "%PROJETO%.env" (
    echo Arquivo .env nao encontrado em %PROJETO%.
    pause
    exit /b 1
)

if not exist "%PROJETO%logs" mkdir "%PROJETO%logs"

echo.
echo ========================================
echo    INICIANDO APLICACAO FLASK
echo ========================================
echo.
echo Aguarde enquanto a aplicacao e iniciada...
echo Todos os logs serao exibidos abaixo:
echo.

cd /d "%PROJETO%"
set FLASK_DEBUG=1
REM Sem segundo processo: evita "Restarting with stat" e PIN do debugger no console
set FLASK_NO_RELOADER=1

REM Abre o navegador padrao apos 3s (servidor ja deve estar escutando)
set "APP_URL=http://127.0.0.1:5000/"
start /min "" powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep -Seconds 3; Start-Process '%APP_URL%'"

"%PYEXE%" -m app

echo.
echo ========================================
echo    APLICACAO FINALIZADA
echo ========================================
echo.
echo Pressione qualquer tecla para fechar esta janela...
pause >nul
