@echo off
REM Script para servir o frontend em HTTP simples

echo ========================================
echo  Iniciando Frontend (HTTP Server)
echo ========================================
echo.

REM Ativar virtual environment
if exist "..\..\.venv\Scripts\activate.bat" (
    call ..\..\.venv\Scripts\activate.bat
)

REM Mudar para diretorio do frontend
cd /d %~dp0

echo [INFO] Servindo frontend em: http://localhost:5173
echo [INFO] Abra o navegador e acesse o link acima
echo.
echo Pressione Ctrl+C para parar o servidor
echo.

REM Iniciar servidor HTTP
python -m http.server 5173
