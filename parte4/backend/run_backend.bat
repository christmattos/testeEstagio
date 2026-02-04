@echo off
echo ========================================
echo  Iniciando Backend FastAPI
echo ========================================
echo.

REM Obter o diretorio raiz do projeto (2 niveis acima)
set "PROJETO_DIR=%~dp0..\.."
cd /d "%PROJETO_DIR%"

REM Verificar e criar virtual environment se necessario
if exist ".venv\Scripts\activate.bat" (
    echo [INFO] Virtual environment encontrado
) else (
    echo [AVISO] Virtual environment nao encontrado!
    echo [INFO] Criando virtual environment...
    echo.
    
    python --version >nul 2>&1
    if errorlevel 1 (
        echo [ERRO] Python nao encontrado!
        echo [INFO] Instale Python 3.10+ de https://www.python.org/
        pause
        exit /b 1
    )
    
    python -m venv .venv
    if errorlevel 1 (
        echo [ERRO] Falha ao criar virtual environment
        pause
        exit /b 1
    )
    echo [OK] Virtual environment criado
    echo.
)

REM Ativar virtual environment
echo [INFO] Ativando virtual environment...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERRO] Falha ao ativar virtual environment
    pause
    exit /b 1
)
echo [OK] Virtual environment ativado
echo.

REM Navegar para pasta do backend
cd parte4\backend

REM Instalar dependencias
echo [INFO] Instalando dependencias...
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERRO] Falha ao instalar dependencias
    pause
    exit /b 1
)
echo [OK] Dependencias instaladas
echo.

echo [INFO] Iniciando servidor FastAPI...
echo [INFO] API disponivel em: http://localhost:8000/api
echo [INFO] Documentacao Swagger: http://localhost:8000/docs
echo [INFO] Documentacao ReDoc: http://localhost:8000/redoc
echo.
echo Pressione Ctrl+C para parar o servidor
echo.

REM Iniciar uvicorn
uvicorn main:app --reload --host 0.0.0.0 --port 8000

REM Se o uvicorn falhar, pausar para ver o erro
if errorlevel 1 (
    echo.
    echo [ERRO] Falha ao iniciar o servidor
    echo.
    echo Verifique se:
    echo   - O MySQL esta rodando
    echo   - As dependencias estao instaladas
    echo   - A porta 8000 nao esta em uso
    echo.
    pause
)
