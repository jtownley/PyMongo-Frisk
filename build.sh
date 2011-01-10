#!/bin/bash
pip -E venv install
source venv/bin/activate

cd src
python --version
python setup.py sdist

pip install --upgrade dist/PyMongo-Frisk*.tar.gz
pip install mock==0.7.0b4

python tests.py

if [ $? == 0 ]
then
  mv ../src/dist/*.tar.gz ..
else
    exit 668
fi
