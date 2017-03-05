#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
# real time mode
python $FINOPT_HOME/comms/tws_gateway.py $FINOPT_HOME/config/app.cfg
