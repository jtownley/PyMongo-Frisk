@echo off
pip -E venv install
call venv\Scripts\activate.bat

cd src
python --version
python setup.py sdist

pip install --upgrade dist\PyMongo-Frisk*.zip
pip install mock==0.7.0b4

python tests.py

IF %ERRORLEVEL%==0 (move dist\*.zip ..) ELSE (exit 668)
