@echo off
echo ========================================
echo  SETUP - Teste Estagio Intuitive Care
echo ========================================
echo.

REM Verificar se Python esta instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado!
    echo [INFO] Instale Python 3.10 ou superior de https://www.python.org/
    pause
    exit /b 1
)

echo [OK] Python encontrado
python --version
echo.

REM Verificar se MySQL esta instalado e rodando
echo [INFO] Verificando MySQL...
mysql --version >nul 2>&1
if errorlevel 1 (
    echo [AVISO] MySQL nao encontrado no PATH
    echo [INFO] Certifique-se de que o MySQL esta instalado e rodando
)
echo.

REM Criar virtual environment
echo [INFO] Criando virtual environment (.venv)...
if exist ".venv" (
    echo [INFO] .venv ja existe, pulando criacao
) else (
    python -m venv .venv
    if errorlevel 1 (
        echo [ERRO] Falha ao criar virtual environment
        pause
        exit /b 1
    )
    echo [OK] Virtual environment criado
)
echo.

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

REM Atualizar pip
echo [INFO] Atualizando pip...
python -m pip install --upgrade pip
echo.

REM Instalar dependencias de cada parte
echo [INFO] Instalando dependencias...
echo.

echo [1/4] Parte 1 - Integracao ANS
if exist "parte1\requirements.txt" (
    pip install -r parte1\requirements.txt
)

echo.
echo [2/4] Parte 2 - Transformacao e Validacao
if exist "parte2\requirements.txt" (
    pip install -r parte2\requirements.txt
)

echo.
echo [3/4] Parte 3 - Banco de Dados
if exist "parte3\requirements.txt" (
    pip install -r parte3\requirements.txt
)

echo.
echo [4/4] Parte 4 - Backend FastAPI
if exist "parte4\backend\requirements.txt" (
    pip install -r parte4\backend\requirements.txt
)

echo.
echo ========================================
echo  SETUP CONCLUIDO!
echo ========================================
echo.
echo Proximos passos:
echo.
echo 1. Configure o MySQL (usuario: root, senha: 1234)
echo 2. Execute as partes do teste em ordem:
echo    - Parte 1: cd parte1 ^&^& python integracao_ans.py
echo    - Parte 2: cd parte2 ^&^& python transformacao_validacao.py
echo    - Parte 3: cd parte3 ^&^& python load_data.py
echo    - Parte 4: cd parte4\backend ^&^& run_backend.bat
echo.
echo Para mais detalhes, veja o README.md
echo.
pause
