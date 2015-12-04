#!/bin/bash
#ROOT={replace-path}
ROOT=~/l1304/production/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/options_data.py $ROOT/config/app.cfg
