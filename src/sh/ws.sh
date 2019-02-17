#!/bin/bash



HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	FINOPT_HOME=~/workspace/finopt/src
elif [ $HOST == 'vorsprung' ]; then
	FINOPT_HOME=~/workspace/finopt/src	
		
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi


export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
#python $FINOPT_HOME/ws/ws_server.py  -c -g AE1  
python $FINOPT_HOME/ws/ws_server.py   -g AE1 -f ../config/ws.cfg 
