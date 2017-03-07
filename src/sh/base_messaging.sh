#!/bin/bash
#FINOPT_HOME=~/l1304/workspace/finopt/src/
FINOPT_HOME=~/ironfly-workspace/finopt/src/
ROOT=$FINOPT_HOME
FINDATA=$ROOT/../data 
SRC=$ROOT
export PYTHONPATH=$SRC:$PYTHONPATH

python $FINOPT_HOME/comms/test/base_messaging.py $1 $2

