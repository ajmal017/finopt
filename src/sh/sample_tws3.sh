#!/bin/bash


HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	FINOPT_HOME=~/workspace/finopt/src
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH

python $FINOPT_HOME/comms/sample_tws_client.py 3