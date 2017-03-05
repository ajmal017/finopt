#!/bin/bash
ROOT=/home/itchyape/production/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/portfolio.py $ROOT/config/app.cfg
#python $ROOT/finopt/portfolio-2017.py $ROOT/config/app.cfg
