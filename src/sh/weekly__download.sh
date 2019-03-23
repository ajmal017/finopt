#!/bin/bash

echo "This script downloads daily market statistics from hkex website"

HOST=$(hostname)
if [ $HOST == 'hkc-larryc-vm1' ]; then
	FINOPT_HOME=~/ironfly-workspace/finopt/src
elif [ $HOST == 'astron' ]; then
	FINOPT_HOME=~/workspace/finopt/src
else
	FINOPT_HOME=~/l1304/workspace/finopt-ironfly/finopt/src
fi


export PYTHONPATH=$FINOPT_HOME:$PYTHONPATH

python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190301 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190304 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190305 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190306 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190307 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190308 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190311 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190312 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190313 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190314 --download_types abcdefghijklm 
python $FINOPT_HOME/hkex/daily_download.py -f ../config/daily_download.cfg --day 190315 --download_types abcdefghijklm 
