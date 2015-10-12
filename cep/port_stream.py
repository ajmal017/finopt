import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from numpy import *
import pylab
from scipy import stats
import time, datetime
import threading
import time
import os
#from finopt import ystockquote
from comms.epc import ExternalProcessComm

 
##
##
##
## This example demonstrates the use of accumulators and broadcast 
## and how to terminate spark running jobs
## 
## it also demonstrates how to send alerts via xmpp
## (requires prosody server running and redisQueue)
##
##

##
##
## insert the path so spark-submit knows where
## to look for a file located in a given directory
##
## the other method is to export PYTHONPATH before 
## calling spark-submit
##
# import sys
# sys.path.insert(0, '/home/larry-13.04/workspace/finopt/cep')
print sys.path


#import optcal
import json
import numpy
#from finopt.cep.redisQueue import RedisQueue
from comms.redisQueue import RedisQueue
from comms.alert_bot import AlertHelper


def f1(time, rdd):
    lt =  rdd.collect()
    if not lt:
        return
    print '**** f1'
    print lt
    print '**** end f1'
    f = open('/home/larry/l1304/workspace/finopt/data/mds_files/std/std-20151007.txt', 'a') # % datetime.datetime.now().strftime('%Y%m%d%H%M'), 'a')
    msg = ''.join('%s,%s,%s,%s,%s\n'%(s[0], s[1][0][0].strftime('%Y-%m-%d %H:%M:%S.%f'),s[1][0][1],s[1][0][2], s[1][1]) for s in lt)
    f.write(msg)
    d = Q.value
    
    # return rdd tuple (-,((-,-),-)): name = 0--, time 100, sd 101, mean 102, vol 11-
    
    for s in lt:
        if s[0].find('HSI-20151029-0') > 0 and (s[1][0][1] > 4.5 or s[1][1] > 100000):      
            msg  = 'Unusal trading activity: %s (SD=%0.2f, mean px=%d, vol=%d) at %s\n'\
                 % (s[0], \
                    s[1][0][1], s[1][0][2],\
                    s[1][1],\
                    s[1][0][0].strftime('%m-%d %H:%M:%S'))   
            q = RedisQueue(d['alert_bot_q'][1], d['alert_bot_q'][0], d['host'], d['port'], d['db'])
            q.put(msg)
            

def f2(time, rdd):
    lt =  rdd.collect()
    if lt:
        change = lt[0][0]
        d = Q.value
        print '********** f2'
        print lt[0][0], Threshold.value, lt[0][1]
        print '********** end f2'

        
        if change > Threshold.value:
            msg = 'Stock alert triggered: %0.6f, mean: %0.2f' % (change, lt[0][1])
            print msg
#             q = RedisQueue(d['alert_bot_q'][1], d['alert_bot_q'][0], d['host'], d['port'], d['db'])
#             q.put(msg)
    


    

# to run from command prompt
# 0. start kafka broker
# 1. edit subscription.txt and prepare 2 stocks
# 2. run ib_mds.py 
# 3. spark-submit  --jars spark-streaming-kafka-assembly_2.10-1.4.1.jar ./alerts/pairs_corr.py vsu-01:2181 

# http://stackoverflow.com/questions/3425439/why-does-corrcoef-return-a-matrix
# 

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: %s <broker_list ex: vsu-01:2181>  " % sys.argv[0])
        print("Usage: to gracefully shutdown type echo 1 > /tmp/flag at the terminal")
        exit(-1)


    app_name = "portfolio.stream"
    sc = SparkContext(appName= app_name) #, pyFiles = ['./cep/redisQueue.py'])
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint('/home/larry-13.04/workspace/finopt/log/checkpoint')



    brokers, qname, id, fn  = sys.argv[1:]
    id = int(id)
    

    NumProcessed = sc.accumulator(0)
    
    

    
    Q = sc.broadcast({ \
                      'rname': 'rname', 'qname': qname, 'namespace': 'mdq', 'host': 'localhost', 'port':6379, 'db': 3, 'alert_bot_q': ('alert_bot', 'chatq')})
    
    
#     s = {v:1 for k,v in ExternalProcessComm.EPC_TOPICS.iteritems()}
#     print s
    kvs = KafkaUtils.createStream(ssc, brokers, app_name, \
                                  {v:1 for k,v in ExternalProcessComm.EPC_TOPICS.iteritems()}) #{'ib_tick_price':1, 'ib_tick_size':1})

    lns = kvs.map(lambda x: x[1]).map(lambda x: json.loads(x))
    ps = lns.filter(lambda x: x)
    lns.pprint()
#     ps = lns.map(lambda x: json.loads(x))\
#             .filter(lambda x: (x['typeName'] == 'tickPrice'))\
#             .map(lambda x: (x['contract'], (x['ts'], x['price']) ))\
#             .groupByKeyAndWindow(12, 10, 1)
# 
#     sps.foreachRDD(f1)
# 
#     sps.pprint()
    #trades.foreachRDD(eval(fn))
    
        
    def do_work():

        while 1:
            # program will stop after processing 40 rdds
#             if NumProcessed.value == 70:
#                 break            
            # program will stop on detecting a 1 in the flag file
            try:
                f = open('/tmp/flag')
                l = f.readlines()
                print 'reading %s' % l[0]
                if '1' in l[0]:
                    os.remove('/tmp/flag') 
                    print 'terminating..........'        
                    ssc.stop(True, False) 
                    sys.exit(0)          
                f.close()
                time.sleep(2)
            except IOError:
                continue
            
            
        
    t = threading.Thread(target = do_work, args=())
    t.start()
    ssc.start()
    ssc.awaitTermination()
    

