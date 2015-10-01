#!/bin/bash
ROOT={replace-path}
export PYTHONPATH=$ROOT/src
python $ROOT/src/finopt/options_data.py $ROOT/src/config/app.cfg
