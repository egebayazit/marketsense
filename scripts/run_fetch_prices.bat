@echo off
setlocal
rem %~dp0 = C:\...\marketsense\scripts\
set "REPO=%~dp0.."
if exist "%REPO%\.venv\Scripts\activate.bat" call "%REPO%\.venv\Scripts\activate.bat"
python "%REPO%\scripts\jobs\fetch_prices.py"
endlocal
