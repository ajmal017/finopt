import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from numpy import *
import pylab
from scipy import stats

import threading
import time
import os
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



def f1(time, rdd):
    lt =  rdd.collect()
    for l in lt:
        print ''.join('%s {%s}' % (l[0], ','.join('%f'% e for e in l[1])))
        xx = [e for e in l[1]]
        print xx
        if len(xx) > 1:
            first = xx[0]
            last_pos = len(xx) - 1
            last = xx[last_pos]
            change = (last - first) / last   
            print '%f' % change
    #rint '\n'.join ('%s %s'% (l[0], ''.join(('%f'% e) for e in l[1])) for l in lt)

def persist(time, rdd):
        #print (time, rdd)
        #lt =  (rdd.collect())
        
        rdd.saveAsTextFile("./rdd/rdd-%s-%03d" % (Q.value['rname'], NumProcessed.value))
        print 'process................... %d' % NumProcessed.value
        NumProcessed.add(1)
        #pass
        #print '\n'.join ('%d %s'% (l[0], ''.join(('%f'% e) for e in l[1])) for l in list) 
 
def simple(time, rdd):
        lt =  (rdd.collect())
        
        if lt:
            
            first = lt[0][1][0]
            last_pos = len(lt) - 1
            last = lt[last_pos][1][0]
            change = (last - first) / last
            msg = '%0.6f, %0.2f, %0.2f, %0.2f' % (change, first, last, last_pos)
            print 'process............. %d {%s}' % (NumProcessed.value, msg)
            if abs(change) > Threshold.value:
                d = Q.value
                q = RedisQueue(d['alert_bot_q'][1], d['alert_bot_q'][0], d['host'], d['port'], d['db'])
                q.put(msg)
            
            
        NumProcessed.add(1)
        #print (time, rdd)

       
        
       

def cal_trend(time, rdd):


    def detect_trend(x1, y1, num_points_ahead, ric):
        z4 = polyfit(x1, y1, 3) 
        p4 = poly1d(z4) # construct the polynomial 
        #print y1
        
        z5 = polyfit(x1, y1, 4)
        p5 = poly1d(z5)
        
        extrap_y_max_limit = len(x1) * 2  # 360 days
        x2 = linspace(0, extrap_y_max_limit, 50) # 0, 160, 100 means 0 - 160 with 100 data points in between
        pylab.switch_backend('agg') # switch to agg backend that support writing in non-main threads
        pylab.plot(x1, y1, 'o', x2, p4(x2),'-g', x2, p5(x2),'-b')
        pylab.legend(['%s to fit' % ric, '4th degree poly', '5th degree poly'])
        #pylab.axis([0,160,0,10])
#         
        pylab.axis([0,len(x1)*1.1, min(y1)*0.997,max(y1)*1.002])  # first pair tells the x axis boundary, 2nd pair y axis boundary 
        
        # compute the slopes of each set of data points
        # sr - slope real contains the slope computed from real data points from d to d+5 days
        # s4 - slope extrapolated by applying 4th degree polynomial
        y_arraysize = len(y1)
#         s4, intercept, r_value, p_value, std_err = stats.linregress(range(0,num_points_ahead), [p4(i) for i in range(y_arraysize,y_arraysize + num_points_ahead )])
#         s5, intercept, r_value, p_value, std_err = stats.linregress(range(0,num_points_ahead), [p5(i) for i in range(y_arraysize,y_arraysize + num_points_ahead )])
        s4, intercept, r_value, p_value, std_err = stats.linregress(x1, y1)
        s5, intercept, r_value, p_value, std_err = stats.linregress(x1, y1)
        
          
        rc = (1.0 if s4 > 0.0 else 0.0, 1.0 if s5 > 0.0 else 0.0)
        print s4, s5, rc, y_arraysize
        #pylab.show()
        pylab.savefig('../data/extrapolation/%s-%s.png' % (ric, time))
        pylab.close()

        d = Q.value
        q = RedisQueue(d['qname'], d['namespace'], d['host'], d['port'], d['db'])
        q.put((time, y1, s4, s5))

        if abs(s4) > Threshold.value:
            d = Q.value
            q = RedisQueue(d['alert_bot_q'][1], d['alert_bot_q'][0], d['host'], d['port'], d['db'])
            q.put('%s %0.2f %0.2f' % (ric, s4, s5))

        return rc

    ls = rdd.collect()  
#     print [y[1][0] for y in ls]
#     print len(ls), [range(len(ls))]
#     print len([y[1][0] for y in ls])
    if ls:
        rc = detect_trend(range(len(ls)), [y[1][0] for y in ls], 5, '_HSI')
    

# to run from command prompt
# 0. start kafka broker
# 1. edit subscription.txt and prepare 2 stocks
# 2. run ib_mds.py 
# 3. spark-submit  --jars spark-streaming-kafka-assembly_2.10-1.4.1.jar ./alerts/pairs_corr.py vsu-01:2181 

# http://stackoverflow.com/questions/3425439/why-does-corrcoef-return-a-matrix
# 

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: %s <broker_list ex: vsu-01:2181>  <rdd_name> <tick id> <fn name>" % sys.argv[0])
        print("Usage: to gracefully shutdown type echo 1 > /tmp/flag at the terminal")
        exit(-1)

    app_name = "Momentum"
    sc = SparkContext(appName= app_name) #, pyFiles = ['./cep/redisQueue.py'])
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint('../checkpoint')

    

    brokers, qname, id, fn  = sys.argv[1:]
    id = int(id)
    
    #
    # demonstrate how to use broadcast variable
    #
    NumProcessed = sc.accumulator(0)
    Q = sc.broadcast({'rname': 'rname', 'qname': qname, 'namespace': 'mdq', 'host': 'localhost', 'port':6379, 'db': 3, 'alert_bot_q': ('msg_bot', 'chatq')})
    Threshold = sc.broadcast(0.00015)
    #kvs = KafkaUtils.createDirectStream(ssc, ['ib_tick_price', 'ib_tick_size'], {"metadata.broker.list": brokers})
    kvs = KafkaUtils.createStream(ssc, brokers, app_name, {'ib_tick_price':1, 'ib_tick_size':1})

    lines = kvs.map(lambda x: x[1])
#     s1 = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == id and x['typeName']== 'tickPrice'))\
#                 .filter(lambda x: (x['field'] == 4))\
#                 .map(lambda x: (id, x['price'])).window(60, 30)
#                 
#     s2 = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == id and x['typeName']== 'tickSize'))\
#                 .filter(lambda x: (x['field'] == 5))\
#                 .map(lambda x: (id, x['size'])).window(60, 30)
#                 
#     
#     
#                 
#     trades = s1.join(s2)
    s1 = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] in [1,2] and x['typeName']== 'tickPrice'))\
                .filter(lambda x: (x['field'] == 4))\
                .map(lambda x: (x['tickerId'], x['price'])).reduceByKey(lambda x,y: (x+y)/2).groupByKeyAndWindow(30, 20, 1)    
    
    
    s1.foreachRDD(f1)
    #trades.pprint()
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
                    break
                f.close()
                time.sleep(2)
            except IOError:
                continue
            
        print 'terminating..........'        
        ssc.stop(True, False) 
        os.remove('/tmp/flag') 
        sys.exit()          
            
        
    t = threading.Thread(target = do_work, args=())
    t.start()
    ssc.start()
    ssc.awaitTermination()

