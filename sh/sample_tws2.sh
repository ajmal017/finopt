#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/comms/sample_tws_client.py 2
