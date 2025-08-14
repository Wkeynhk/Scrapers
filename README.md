### Требования
- **Python 3.10+**
- Windows PowerShell (команды ниже рассчитаны на Windows)

### AnkerGames
1) Установка зависимостей в изолированное окружение:
```powershell
cd AnkerGames
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```
2) Запуск парсера:
```powershell
python ankergames.py --limit 5 --output ankergames.json
```
- **--limit**: ограничить число игр для парсинга (опционально)
- **--output**: путь к выходному JSON (по умолчанию `ankergames.json`)

### Repack-Games
1) Установка зависимостей:
```powershell
cd RepackGames
pip install -r requirements.txt
```
2) Запуск парсера:
```powershell
python RepackGames.py
```

