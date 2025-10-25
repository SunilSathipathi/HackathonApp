@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Copy env template if missing
IF NOT EXIST .env (
  IF EXIST .env.example (
    copy /Y .env.example .env >nul
  )
)

REM Create venv if missing (try py then python)
IF NOT EXIST venv (
  where py >nul 2>nul && ( py -m venv venv ) || ( python -m venv venv )
)

REM Activate venv
CALL venv\Scripts\activate.bat

REM Install deps
pip install -r requirements.txt

REM Run app
python main.py