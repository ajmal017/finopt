#!/bin/bash
ROOT=~/mchan927/finopt
export PYTHONPATH=$ROOT:$PYTHONPATH
# real time mode
python $ROOT/cep/ib_mds.py $ROOT/config/mds.cfg
# replay mode
#python $FINOPT_HOME/cep/ib_mds.py -r $FINOPT_HOME/../data/mds_files/20151006 $FINOPT_HOME/config/mds.cfg

