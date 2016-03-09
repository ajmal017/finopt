#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
# real time mode
python $FINOPT_HOME/finopt/options_chain.py $FINOPT_HOME/config/app.cfg
