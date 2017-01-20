#!/bin/bash
ROOT=~/production/finopt
export PYTHONPATH=$ROOT

kill -9  $(cat $ROOT/options_data.pid)
