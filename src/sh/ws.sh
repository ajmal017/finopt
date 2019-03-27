#!/bin/bash

WS_CFG=ws.cfg

HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	FINOPT_HOME=~/workspace/finopt/src
elif [ $HOST == 'vorsprung' ]; then
	FINOPT_HOME=~/workspace/finopt/src	
		
elif [ $HOST == 'vsu-longhorn' ]; then
    FINOPT_HOME=~/pyenvs/ironfly/finopt/src
    source /home/vuser-longhorn/pyenvs/finopt/bin/activate
    WS_CFG=ws_prd.cfg
							
fi


export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/ws/ws_server.py  -c  -g AE1 -f ../config/$WS_CFG 
 
