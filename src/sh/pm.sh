#!/bin/bash


PM_CFG=pm.cfg
HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	FINOPT_HOME=~/workspace/finopt/src
elif [ $HOST == 'vsu-longhorn' ]; then
    FINOPT_HOME=~/pyenvs/ironfly/finopt/src
    source /home/vuser-longhorn/pyenvs/finopt/bin/activate
    PM_CFG=pm_prd.cfg						
fi
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/rethink/portfolio_monitor.py  -c -g PM1  -f ../config/$PM_CFG
#python $FINOPT_HOME/rethink/portfolio_monitor.py -g PM1
