#!/bin/bash

MDS_CFG=mds.cfg 
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
    #SYMBOLS_PATH=/home/laxaurus/workspace/fpydevs/dat/symbols/goog.txt
        SYMBOLS_PATH=$FINOPT_HOME/../../../dat/symbols/instruments.txt
    REPLAY_PATH=/home/laxaurus/workspace/fpydevs/dat/mds_files


elif [ $HOST == 'vsu-longhorn' ]; then
        FINOPT_HOME=~/pyenvs/ironfly/finopt/src
        source /home/vuser-longhorn/pyenvs/finopt/bin/activate
        MDS_CFG=mds_prd.cfg
elif [ $HOST == 'vsu-vortify' ]; then
        FINOPT_HOME=~/workspace/fpydevs/eclipse/finopt/src
        source /home/vuser-vortify/workspace/fpydevs/env/bin/activate
        MDS_CFG=mds_avant.cfg
        SYMBOLS_PATH=$FINOPT_HOME/../../../dat/symbols/goog.txt
        #SYMBOLS_PATH=$FINOPT_HOME/../../../dat/symbols/instruments.txt
fi
									
						
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
#  
# clear all topic offsets and erased saved subscriptions

# record and publish ticks
python $FINOPT_HOME/cep/ib_mds.py -s $SYMBOLS_PATH -f $FINOPT_HOME/config/$MDS_CFG 

# replay ticks
#python $FINOPT_HOME/cep/ib_mds.py -r $REPLAY_PATH -f $FINOPT_HOME/config/$MDS_CFG


#
# clear offsets in redis / reload saved subscription entries
#python $FINOPT_HOME/comms/ibgw/mds.py  -c -f $FINOPT_HOME/config/mds.cfg 


# restart gateway keep the redis offsets but erase the subscription entries
#python $FINOPT_HOME/comms/ibgw/mds.py  -r -f $FINOPT_HOME/config/mds.cfg 

# normal restart - keep the offsets and reload from saved subscription entries
#python $FINOPT_HOME/comms/ibgw/mds.py   -f $FINOPT_HOME/config/mds.cfg 
