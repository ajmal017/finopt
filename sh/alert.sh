#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
python $FINOPT_HOME/comms/alert_bot.py $FINOPT_HOME/config/app.cfg

