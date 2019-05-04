#!/bin/bash

TWS_GATEWAY_CFG=tws_gateway.cfg 
HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'vorsprung' ]; then
	FINOPT_HOME=~/workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	#	FINOPT_HOME=~/workspace/finopt/src

	# virtual env
	FINOPT_HOME=~/workspace/fpydevs/eclipse/finopt/src
	source /home/laxaurus/workspace/fpydevs/env/bin/activate


elif [ $HOST == 'vsu-longhorn' ]; then
        FINOPT_HOME=~/pyenvs/ironfly/finopt/src
        source /home/vuser-longhorn/pyenvs/finopt/bin/activate
        TWS_GATEWAY_CFG=tws_gateway_prd.cfg
elif [ $HOST == 'vsu-vortify' ]; then
        FINOPT_HOME=~/workspace/fpydevs/eclipse/finopt/src
        source /home/vuser-vortify/workspace/fpydevs/env/bin/activate
        TWS_GATEWAY_CFG=tws_gateway_avant.cfg
fi
									
						
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
#  
# clear all topic offsets and erased saved subscriptions


python $FINOPT_HOME/comms/ibgw/tws_gateway.py -r -c -f $FINOPT_HOME/config/$TWS_GATEWAY_CFG


#
# clear offsets in redis / reload saved subscription entries
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py  -c -f $FINOPT_HOME/config/tws_gateway.cfg 


# restart gateway keep the redis offsets but erase the subscription entries
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py  -r -f $FINOPT_HOME/config/tws_gateway.cfg 

# normal restart - keep the offsets and reload from saved subscription entries
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py   -f $FINOPT_HOME/config/tws_gateway.cfg 
