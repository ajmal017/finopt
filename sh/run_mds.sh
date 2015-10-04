#!/bin/bash
ROOT=$FINOPT_HOME
export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH
# real time mode
#python $FINOPT_HOME/cep/ib_mds.py $FINOPT_HOME/config/mds.cfg
# replay mode
python $FINOPT_HOME/cep/ib_mds.py -r $FINOPT_HOME/../data/mds_files/large_up_1002 $FINOPT_HOME/config/mds.cfg

