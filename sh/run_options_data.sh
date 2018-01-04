#!/bin/bash
#ROOT={replace-path}
ROOT=~/mchan927/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/options_data.py $ROOT/config/app.cfg
