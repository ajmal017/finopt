import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#import optcal
import json
import numpy



def process(time, rdd):
        #print (time, rdd)
        lt =  (rdd.collect())
        #print '\n'.join ('%d %s'% (l[0], ''.join(('%f'% e) for e in l[1])) for l in list) 
        if len(lt) == 2:
            a = list(lt[0][1])
            b = list(lt[1][1])
            #print a, b
            corr = 0.0
            if len(a) > 1 and len(b) > 1:                
                if len(a) > len(b):
                    corr= numpy.corrcoef(a[:len(b)], b)
                else:
                    corr= numpy.corrcoef(b[:len(a)], a)
                    
                print "%s corr---> %f" % (time.strftime('%Y%m%d %H:%M:%S'), corr.tolist()[0][1])
            #print numpy.corrcoef(list(lt[0][1]), list(lt[1][1]))


       

# to run from command prompt
# 0. start kafka broker
# 1. edit subscription.txt and prepare 2 stocks
# 2. run ib_mds.py 
# 3. spark-submit  --jars spark-streaming-kafka-assembly_2.10-1.4.1.jar ./alerts/pairs_corr.py vsu-01:2181 

# http://stackoverflow.com/questions/3425439/why-does-corrcoef-return-a-matrix
# 

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
    uso = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == 1 and x['typeName']== 'tickPrice'))\
                .map(lambda x: (1, x['price'])).window(8, 6)
    dug = lines.map(lambda line: json.loads(line)).filter(lambda x: (x['tickerId'] == 2 and x['typeName']== 'tickPrice'))\
                .map(lambda x: (2, x['price'])).window(8, 6)
                
                
    pair = uso.union(dug).groupByKey()
    # sample values are empty, one element, and 2 elements
    #(1, <pyspark.resultiterable.ResultIterable object at 0x7fae53a187d0>)
    #(2, <pyspark.resultiterable.ResultIterable object at 0x7fae53a18c50>)

    
    
    #pair.pprint()
    pair.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()

