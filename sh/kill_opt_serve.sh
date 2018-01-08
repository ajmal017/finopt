#!/bin/bash
ROOT=~/mchan927/finopt
export PYTHONPATH=$ROOT

kill -9  $(cat $ROOT/opt_serve.pid)
