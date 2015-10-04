from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from numpy import *
import pylab
from scipy import stats
import json
import numpy
import time, datetime
from os import listdir
from os.path import isfile, join
from optparse import OptionParser
import sys
path = '/home/larry/l1304/workspace/finopt/data/mds_files/large_up_1002/ibkdump-20151002105314.txt'
path = '/home/larry/l1304/workspace/finopt/data/mds_files/large_up_1002/ibkdump-20151002110412.txt'



#
# demo how to compute standard deviation in a RDD

 



def process_msg(file):

    try:
        md = sc.textFile(file)
        mdl = md.map(lambda lines: (lines.split('|'))).map(lambda x: json.loads(x[1]))\
                .filter(lambda x: (x['typeName'] == 'tickPrice' and x['contract'] == "HSI-20151029-0--FUT-HKD-102"))\
                .map(lambda x: x['price'])
        sd = numpy.std(mdl.collect())
        #print file[len(file)-20:], sd, mdl.count()
        return (sd, mdl.count())
    except:
        return None
    
def process_msg_by_key(file):

    


    try:
        md = sc.textFile(file)    
        print file
        mdl = md.map(lambda lines: (lines.split('|'))).map(lambda x: json.loads(x[1]))
        
                
                
        #mdp = mdl.filter(lambda x: (x['typeName'] == 'tickPrice')) and x['contract'] in ["HSI-20151029-0--FUT-HKD-102"]))\
        mdp = mdl.filter(lambda x: (x['typeName'] == 'tickPrice'))\
                .map(lambda x: (x['contract'], (x['ts'], x['price']) )).groupByKey()
                
        #mds = mdl.filter(lambda x: (x['typeName'] == 'tickSize'  and x['contract'] in ["HSI-20151029-0--FUT-HKD-102"]))\
        mds = mdl.filter(lambda x: (x['typeName'] == 'tickSize'))\
                .map(lambda x: (x['contract'], (x['ts'], x['size']) )).groupByKey()                
        
        sdp = mdp.map(lambda x: (x[0],\
                                 (datetime.datetime.fromtimestamp( [a[0] for a in x[1]][0]  ),\
                                  numpy.std([a[1] for a in x[1]]),\
                                  numpy.mean([a[1] for a in x[1]]))\
                                ))
        
        sds = mds.map(lambda x: (x[0],\
                                 (datetime.datetime.fromtimestamp( [a[0] for a in x[1]][0]  ),\
                                  numpy.std([a[1] for a in x[1]]),\
                                  numpy.mean([a[1] for a in x[1]]))\
                                )) 
         
        #print sds.take(2)
        sdsp = sdp.cogroup(sds)
        elems = sdsp.collect()
        for e in elems:            
            print '%s %s %s' % (e[0], ''.join('[%s %0.2f %0.2f]'%(p[0],p[1],p[2]) for p in e[1][0]), ''.join('[%s %0.2f %0.2f]'%(p[0],p[1],p[2]) for p in e[1][1]))
        return sdsp 
    except:
        return 
    

if __name__ == '__main__':
    
    parser = OptionParser()
#     parser.add_option("-r", "--replay",
#                       dest="replay_dir",
#                       help="replay recorded mds files stored in the specified directory")
                      
    
    options, arguments = parser.parse_args()

    #print options, arguments
    
    if len(sys.argv) < 2:
        print("Usage: %s [options] <dir>" % sys.argv[0])
        exit(-1)    
    
    
    sc = SparkContext(appName= 't1')    
    #dir_loc = '/home/larry/l1304/workspace/finopt/data/mds_files/large_up_1002'
    dir_loc = arguments[0]
    files = sorted([ join(dir_loc,f) for f in listdir(dir_loc) if isfile(join(dir_loc,f)) ])

    a = [(process_msg_by_key(f)) for f in files]
    #print a
    #print ''.join('%s,%s,%s\n' % (aa[0], aa[1], aa[2]) if aa <> None else '' for aa in a )
