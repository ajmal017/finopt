#!/bin/bash


HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/comms/ibgw/base_messaging.py $1 $2
