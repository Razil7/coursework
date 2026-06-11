@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================================
echo   Демонстрационный стенд - асинхронные процессы
echo   Сейчас откроется браузер с интерфейсом.
echo   Чтобы остановить - закройте это окно или нажмите Ctrl+C.
echo ============================================================
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "gui\server.py"
) else (
  python "gui\server.py"
)
pause
