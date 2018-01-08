#!/bin/bash
#ROOT={replace-path}
ROOT=~/mchan927/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/portfolio.py $ROOT/config/app.cfg
