#!/bin/bash
ROOT={replace-path}
export PYTHONPATH=$ROOT/src
python $ROOT/src/finopt/opt_serve.py $ROOT/src/config/app.cfg
