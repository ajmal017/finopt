#!/bin/bash
#ROOT={replace-path}
ROOT=~/l1304/workspace/finopt
export PYTHONPATH=$ROOT/src
python $ROOT/src/finopt/options_data.py $ROOT/src/config/app.cfg
