#!/bin/bash
ROOT=/home/larry/l1304/workspace/finopt/src
export PYTHONPATH=$ROOT
python $ROOT/finopt/portfolio_kf.py $ROOT/config/app.cfg
