#!/bin/bash
ROOT=/home/larry/l1304/production/finopt
export PYTHONPATH=$ROOT
python $ROOT/finopt/portfolio.py $ROOT/config/app.cfg
