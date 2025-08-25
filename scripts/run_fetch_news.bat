@echo off
setlocal
set "REPO=%~dp0.."
if exist "%REPO%\.venv\Scripts\activate.bat" call "%REPO%\.venv\Scripts\activate.bat"
python "%REPO%\scripts\jobs\fetch_news.py"
endlocal
