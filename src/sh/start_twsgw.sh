#!/bin/bash


HOST=$(hostname)
echo $HOST
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
#  
# clear all topic offsets and erased saved subscriptions
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py -r -c -f $FINOPT_HOME/config/tws_gateway.cfg 


#
# clear offsets in redis / reload saved subscription entries
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py  -c -f $FINOPT_HOME/config/tws_gateway.cfg 


# restart gateway keep the redis offsets but erase the subscription entries
python $FINOPT_HOME/comms/ibgw/tws_gateway.py  -r -f $FINOPT_HOME/config/tws_gateway.cfg 

# normal restart - keep the offsets and reload from saved subscription entries
#python $FINOPT_HOME/comms/ibgw/tws_gateway.py   -f $FINOPT_HOME/config/tws_gateway.cfg 
