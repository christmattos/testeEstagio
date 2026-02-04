@echo off
echo ========================================
echo  SETUP - Teste Estagio
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

REM Atualizar pip
echo [INFO] Atualizando pip...
python -m pip install --upgrade pip
echo.

echo.
echo ========================================
echo  SETUP CONCLUIDO!
echo ========================================
echo.
echo Proximos passos:
echo.
echo 1. Configure o MySQL (usuario: root, senha: 1234);
echo 2. Instale TODAS as dependencias;
echo 3. Execute as partes do teste em ordem:
echo    - Parte 1: cd parte1 ^&^& python integracao_ans.py
echo    - Parte 2: cd parte2 ^&^& python transformacao_validacao.py
echo    - Parte 3: cd parte3 ^&^& python load_data.py
echo    - Parte 4: cd parte4\backend ^&^& run_backend.bat
echo.
echo Para mais detalhes, veja o README.md
echo.
pause
