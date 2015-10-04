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

## 
## to run, start kafka server on vsu-01 <administrator, password>
## start ibgw or tws
## edit subscript list
## check the settings in the console
## 

class IbKafkaProducer():
    
    config = None
    con = None
    quit = False
    producer = None
    IB_TICK_PRICE = None
    IB_TICK_SIZE = None
    toggle = False
    persist = {}
    ibh = None
    tlock = None
    ib_conn_status = None
    
    id2contract = {'conId': 1, 'id2contracts': {} }
    
    
    def __init__(self, config, replay = False):
        
        self.config = config
        self.tlock = Lock()
#         host = self.config.get("ib_mds", "ib_mds.gateway").strip('"').strip("'")
#         port = int(self.config.get("ib_mds", "ib_mds.ib_port"))
#         appid = int(self.config.get("ib_mds", "ib_mds.appid.id"))  
        kafka_host = self.config.get("cep", "kafka.host").strip('"').strip("'")
        
        self.persist['is_persist'] = self.config.get("ib_mds", "ib_mds.is_persist")
        self.persist['persist_dir'] =self.config.get("ib_mds", "ib_mds.persist_dir").strip('"').strip("'")
        self.persist['file_exist'] = False
        self.persist['spill_over_limit'] = int(self.config.get("ib_mds", "ib_mds.spill_over_limit"))

        
        IbKafkaProducer.IB_TICK_PRICE = self.config.get("cep", "kafka.ib.topic.tick_price").strip('"').strip("'")
        IbKafkaProducer.IB_TICK_SIZE = self.config.get("cep", "kafka.ib.topic.tick_size").strip('"').strip("'")
#         self.con = ibConnection(host, port, appid)
#         self.con.registerAll(self.on_ib_message)
#         rc = self.con.connect()
#         if rc:
#             self.ib_conn_status = 'OK'
        
#        logging.info('IbKafkaProducer: connection status to IB is %d' % rc)

        logging.info('******* Starting IbKafkaProducer')
        logging.info('IbKafkaProducer: connecting to kafka host: %s...' % kafka_host)
        logging.info('IbKafkaProducer: message mode is async')
        
        client = KafkaClient(kafka_host)
        self.producer = SimpleProducer(client, async=False)
        
        if not replay:
            self.start_ib_connection()
        
#         # start heart beat monitor
#         self.ibh = IbHeartBeat(config)
#         self.ibh.register_listener([self.on_ib_conn_broken])
#         #self.ibh.run()        
    
    def pub_cn_index(self, sec):
        #http://blog.csdn.net/moneyice/article/details/7877030
        # hkHSI, hkHSCEI, hkHSCCI
        
        qs = '0000001,1399001,1399300'
        url = 'http://api.money.126.net/data/feed/%s?callback=ne3587367b7387dc' % qs
        
        while 1:
        
            pg = urllib2.urlopen(url.encode('utf-8'))    
            s = pg.read().replace('ne3587367b7387dc(', '')            
            s = s[:len(s)-2]
             
            d = json.loads(s)
            
            tick_id = 9000
            for k,v in d.iteritems():
                
                
                c_name  = '%s-%s-%s-%s' % (v['type'],v['code'],0,0)

                msg =  "%s,%d,%d,%s,%s" % (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), tick_id ,\
                                                                4, v['price'], c_name)
                print msg
                self.producer.send_messages('my.price', msg.encode('utf-8'))
                tick_id+=1

            sleep(sec)
        
        
    def add_contract(self, tuple):
        print tuple
        c = options_data.ContractHelper.makeContract(tuple)
        self.con.reqMktData(self.id2contract['conId'], c, '', False)
        self.id2contract['id2contracts'][self.id2contract['conId']] = options_data.ContractHelper.makeRedisKeyEx(c)
        self.id2contract['conId']+=1
        
    def on_ib_conn_broken(self, msg):
        logging.error('IbKafkaProducer: connection is broken!')
        self.ib_conn_status = 'ERROR'
        self.tlock.acquire() # this function may get called multiple times
        try:                 # block until another party finishes executing
            if self.ib_conn_status == 'OK': # check status
                return                      # if already fixed up while waiting, return 
            
            self.con.eDisconnect()
    
            
            host = self.config.get("ib_mds", "ib_mds.gateway").strip('"').strip("'")
            port = int(self.config.get("ib_mds", "ib_mds.ib_port"))
            appid = int(self.config.get("ib_mds", "ib_mds.appid.id"))        
            self.con = ibConnection(host, port, appid)
            self.con.registerAll(self.on_ib_message)
            rc = None
            while not rc and not self.quit:
                logging.error('IbKafkaProducer: attempt reconnection!')
                rc = self.con.connect()
                logging.info('IbKafkaProducer: connection status to IB is %d (0-broken 1-good)' % rc)
                sleep(2)
            
            # we arrived here because the connection has been restored
            if not self.quit:
                # resubscribe tickers again!
                self.load_tickers()                
                a = AlertHelper(self.config)
                a.post_msg('ib_mds recovered from broken ib conn, re-subscribe tickers...')            
                logging.debug('on_ib_conn_broken: completed restoration. releasing lock...')
                self.ib_conn_status = 'OK'
            
        finally:
            self.tlock.release()          
        

        
                
    
    def on_ib_message(self, msg):  


        def create_tick_kmessage(msg):
            d = {}
            for t in msg.items():
                d[t[0]] = t[1]
            d['ts'] = time.time()
            d['contract'] = self.id2contract['id2contracts'][msg.tickerId]
            d['typeName'] = msg.typeName
            d['source'] = 'IB'
            return json.dumps(d)

        
        if msg.typeName in ['tickPrice', 'tickSize']:

            t = create_tick_kmessage(msg)             
            logging.debug(t)   
            if self.toggle:
                print t
            self.producer.send_messages(IbKafkaProducer.IB_TICK_PRICE if msg.typeName == 'tickPrice' else IbKafkaProducer.IB_TICK_SIZE, t) 
            if self.persist['is_persist']:
                self.write_message_to_file(t)
                
            
            
#         print ",%0.4f,%0.4f" % (msg.pos, msg.avgCost)
#         self.producer.send_messages('my.topic', "%0.4f,%0.4f" % (msg.pos, msg.avgCost))
        elif msg.typeName in ['error']:
            logging.error('on_ib_message: %s %s' % (msg.errorMsg, self.id2contract['id2contracts'][msg.id] if msg.errorCode == 200 else str(msg.errorCode)) )
        else:
           if self.toggle:
               print msg
                
         
    def write_message_to_file(self, t):
        if self.persist['file_exist'] == False:
            fn = '%s/ibkdump-%s.txt' % (self.persist['persist_dir'], datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            logging.debug('write_message_to_file: path %s', fn)
            self.persist['fp'] = open(fn, 'w')
            self.persist['file_exist'] = True
            self.persist['spill_over_count'] = 0
        
        self.persist['fp'].write('%s|%s\n' % (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), t))
        self.persist['spill_over_count'] +=1
        #print self.persist['spill_over_count']
        if self.persist['spill_over_count'] == self.persist['spill_over_limit']:
            self.persist['fp'].flush()
            self.persist['fp'].close()
            self.persist['file_exist'] = False


    def start_ib_connection(self):
        host = self.config.get("ib_mds", "ib_mds.gateway").strip('"').strip("'")
        port = int(self.config.get("ib_mds", "ib_mds.ib_port"))
        appid = int(self.config.get("ib_mds", "ib_mds.appid.id"))          
        self.con = ibConnection(host, port, appid)
        self.con.registerAll(self.on_ib_message)
        rc = self.con.connect()
        if rc:
            self.ib_conn_status = 'OK'
        logging.info('start_ib_connection: connection status to IB is %d' % rc)
        
        # start heart beat monitor
        self.ibh = IbHeartBeat(config)
        self.ibh.register_listener([self.on_ib_conn_broken])
        self.ibh.run()        
        

    def load_tickers(self, path=None):
        
        self.id2contract = {'conId': 1, 'id2contracts': {} }
        
        
        if path is None:
            path = self.config.get("ib_mds", "ib_mds.subscription.fileloc").strip('"').strip("'")
        logging.info('load_tickers: attempt to open file %s' % path)
        fr = open(path)
        for l in fr.readlines():
            if l[0] <> '#':
                 
                self.add_contract(tuple([t for t in l.strip('\n').split(',')]))
            

    def do_work(self):
        while not self.quit:
            pass
        
    def run_forever(self):
        t = threading.Thread(target = self.do_work, args=())
        t.start()
        self.console()
        print 'pending ib connection to shut down...'
        self.disconnect()
        t.join()
        print 'shutdown complete.'

        
    def disconnect(self):

        self.con.disconnect()
        if 'fp' in self.persist and self.persist['fp']:
            self.persist['fp'].flush()
            self.persist['fp'].close()
        self.quit = True
        self.ibh.shutdown()


    def replay(self, dir_loc):
        
         
        
        def process_msg(fn):
            fp = open(fn)
            logging.info('replay file %s' % fn)
            last_record_ts = None
            for line in fp:
                
                s_msg = line.split('|')[1]
                msg = json.loads(s_msg)
                msg_ts = datetime.datetime.fromtimestamp(msg['ts'])
                interval = (msg_ts - (last_record_ts if last_record_ts <> None else msg_ts)).microseconds / 1000000.0
                
                print '%s %s %s' % (msg_ts.strftime('%Y-%m-%d %H:%M:%S.%f'), s_msg, fn)
                self.producer.send_messages(IbKafkaProducer.IB_TICK_PRICE if msg['typeName'] == 'tickPrice' else IbKafkaProducer.IB_TICK_SIZE, s_msg)
                 
                last_record_ts = msg_ts
                sleep(interval)

        files = sorted([ join(dir_loc,f) for f in listdir(dir_loc) if isfile(join(dir_loc,f)) ])   
                 
        for f in files:
            process_msg(f)
        

    def console(self):
        try:
            while not self.quit:
                print "Available commands are: l - list all subscribed contracts, a <symbol> <sectype> <exch> <ccy>"
                print "                        t - turn/on off  output q - terminate program"
                cmd = raw_input(">>")
                input = cmd.split(' ')
                if input[0]  == "q":
                    print 'quit command received...'
                    self.quit = True
                elif input[0] == 'l' :
                    print ''.join('%s: %s\n' % (k, v) for k, v in self.id2contract['id2contracts'].iteritems())
                elif input[0] == 'a':
                    self.add_contract((input[1],input[2],input[3],input[4],'',0,''))
                elif input[0] == 't':
                    self.toggle = False if self.toggle else True
                else:
                    pass
            
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
            

if __name__ == '__main__':
           
    
    parser = OptionParser()

    parser.add_option("-r", "--replay",
                      dest="replay_dir",
                      help="replay recorded mds files stored in the specified directory")
                      

    
    options, arguments = parser.parse_args()

    #print options, arguments
    
    if len(sys.argv) < 2:
        print("Usage: %s [options] <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= arguments[0]
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0:      
        raise ValueError, "Failed to open config file" 
    
      
    logconfig = eval(config.get("ib_mds", "ib_mds.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)    
    replay = True if options.replay_dir <> None else False 
    ik = IbKafkaProducer(config, replay)
    
    if not replay:
        ik.load_tickers()    
    else:
        ik.replay(options.replay_dir)
        
    ik.run_forever()
    