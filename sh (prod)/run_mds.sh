#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
# real time mode
python $FINOPT_HOME/cep/ib_mds.py $FINOPT_HOME/config/mds.cfg
# replay mode
#python $FINOPT_HOME/cep/ib_mds.py -r $FINOPT_HOME/../data/mds_files/20151006 $FINOPT_HOME/config/mds.cfg

