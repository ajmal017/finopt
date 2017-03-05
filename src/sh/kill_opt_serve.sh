#!/bin/bash
ROOT=~/production/finopt
export PYTHONPATH=$ROOT

kill -9  $(cat $ROOT/opt_serve.pid)
