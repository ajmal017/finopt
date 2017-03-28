#!/bin/bash


HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/rethink/analytics_engine.py  -c -g AE1  
