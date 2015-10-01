#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/cep/ib_mds.py $FINOPT_HOME/config/app.cfg

