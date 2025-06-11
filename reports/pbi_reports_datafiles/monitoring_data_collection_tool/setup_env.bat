@echo off
SETLOCAL

:: Define paths
SET VENV_DIR=.venv
SET REQUIREMENTS_FILE=documentation\requirements.txt

:: Step 1: Create virtual environment
echo Creating virtual environment...
python -m venv %VENV_DIR%

:: Step 2: Activate the environment
echo Activating virtual environment...
CALL %VENV_DIR%\Scripts\activate

:: Step 3: Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

:: Step 4: Install dependencies
echo Installing dependencies from %REQUIREMENTS_FILE%...
pip install -r %REQUIREMENTS_FILE%

:: Step 5: Success confirmation
echo.
echo Environment setup complete!
echo To activate the environment later, run:
echo     CALL %VENV_DIR%\Scripts\activate
echo.

ENDLOCAL
PAUSE
