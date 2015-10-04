#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$ROOT
python $ROOT/finopt/opt_serve.py $ROOT/config/app.cfg
