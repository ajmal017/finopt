# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import ConfigParser
from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
from time import sleep
import time, datetime
from os import listdir
from os.path import isfile, join
from threading import Lock
from comms.ib_heartbeat import IbHeartBeat
import threading, urllib2
from optparse import OptionParser
#from options_data import ContractHelper

import finopt.options_data as options_data
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from comms.alert_bot import AlertHelper


def get_ib_acct_tags(file):
    f = open(file)
    ln = f.readlines()
    return ''.join("'%s'," % s.replace('\n', '').strip(' ') for s in ln)




# copy list of subscribed contracts from the console in options_data
# save them into a text file
# call this function to generate subscription txt that 
# can be processed by ib_mds.py
def create_subscription_file(src, dest):
    
    # improper file content will cause
    # this function to fail
    f = open(src)
    lns = f.readlines()

    a= filter(lambda x: x[0] <> '\n', map(lambda x: x.split(','), lns))
    contracts = map(lambda x: x.split('-'), [c[1] for c in a])
    options = filter(lambda x: x[2] <> 'FUT', contracts)
    futures = filter(lambda x: x[2] == 'FUT', contracts)
    print contracts
    #HSI,FUT,HKFE,HKD,20151029,0,
    futm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'FUT', 'HKFE', 'HKD', x[1], '0', ''), futures)
    outm= map(lambda x: "%s,%s,%s,%s,%s,%s,%s" % (x[0], 'OPT', 'HKFE', 'HKD', x[1], x[2], x[3]), options)
    f1 = open(dest, 'w')
    f1.write(''.join('%s\n'% c for c in outm))
    f1.write(''.join('%s\n'% c for c in futm))
    f1.close()
    

#         newContract.m_symbol = contractTuple[0]
#         newContract.m_secType = contractTuple[1]
#         newContract.m_exchange = contractTuple[2]
#         newContract.m_currency = contractTuple[3]
#         newContract.m_expiry = contractTuple[4]
#         newContract.m_strike = contractTuple[5]
#         newContract.m_right = contractTuple[6]

if __name__ == '__main__':
    #create_subscription_file('/home/larry-13.04/workspace/finopt/log/hsio.txt', '/home/larry-13.04/workspace/finopt/log/subo.txt')
    print get_ib_acct_tags('/home/larry/l1304/workspace/finopt/data/temp_data/ib_account_summary_tags')
    