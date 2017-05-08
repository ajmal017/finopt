#!/bin/bash


HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'vorsprung' ]; then
	FINOPT_HOME=~/workspace/finopt/src	
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
#python $FINOPT_HOME/rethink/portfolio_monitor.py  -c -g PM1  
python $FINOPT_HOME/rethink/portfolio_monitor.py -g PM2 
