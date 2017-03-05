import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#import optcal
import json


def process(time, rdd):
        #print (time, rdd)
        list =  (rdd.collect())
        #print ('process---> %s' %list)
        print '\n'.join ('%s %s'% (l['contract'], l['price']) for l in list) 

def psize(time, rdd):
        list = rdd.collect()
        print '\n'.join ('%s:%s'% (l[0],l[1]) for l in list)
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ib_test02.py <broker_list ex: vsu-01:2181>")
        exit(-1)

    app_name = "IbMarketDataStream"
    sc = SparkContext(appName= app_name)
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint('./checkpoint')



    brokers = sys.argv[1]
    #kvs = KafkaUtils.createDirectStream(ssc, ['ib_tick_price', 'ib_tick_size'], {"metadata.broker.list": brokers})
    kvs = KafkaUtils.createStream(ssc, brokers, app_name, {'ib_tick_price':1, 'ib_tick_size':1})

    lines = kvs.map(lambda x: x[1])
    msg = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == 1 and x['typeName']== 'tickPrice')).window(4, 2)
    size = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == 1 and x['typeName']== 'tickSize'))\
            .map(lambda x: (x['tickerId'], x['size'])).window(4,2) 
    avg = size.reduceByKey(lambda a,b: (a+b)/2).window(4, 2)
    

#    mix.pprint()

    msg.foreachRDD(process)
    avg.foreachRDD(psize)
    ssc.start()
    ssc.awaitTermination()

