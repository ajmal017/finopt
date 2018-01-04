#!/bin/bash
ROOT=~/mchan927/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/opt_serve.py $ROOT/config/app.cfg & echo $! > $ROOT/opt_serve.pid

echo $(cat $ROOT/opt_serve.pid)
