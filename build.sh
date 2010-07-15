#!/bin/bash
pip -E venv install
source venv/bin/activate

cd src
python --version
python setup.py sdist

pip install --upgrade dist/pymongo_frisk*.tar.gz
pip install mock==0.6.0

python tests.py

if [ $? == 0 ]
then
  mv ../src/dist/*.tar.gz ..
else
    exit 668
fi
