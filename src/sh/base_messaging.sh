#!/bin/bash
<<<<<<< HEAD
FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src/
=======
#FINOPT_HOME=~/l1304/workspace/finopt/src/
FINOPT_HOME=~/ironfly-workspace/finopt/src/
>>>>>>> branch 'ironfly' of https://github.com/laxaurus/finopt.git
ROOT=$FINOPT_HOME
FINDATA=$ROOT/../data 
SRC=$ROOT
export PYTHONPATH=$SRC:$PYTHONPATH

python $FINOPT_HOME/comms/test/base_messaging.py $1 $2

