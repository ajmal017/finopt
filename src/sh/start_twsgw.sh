#!/bin/bash
FINOPT_HOME=~/ironfly-workspace/finopt/src
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH

python $FINOPT_HOME/comms/ibgw/tws_gateway.py $FINOPT_HOME/config/tws_gateway.cfg 

