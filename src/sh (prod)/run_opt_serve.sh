#!/bin/bash
ROOT=~/l1304/production/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/opt_serve.py $ROOT/config/app.cfg
