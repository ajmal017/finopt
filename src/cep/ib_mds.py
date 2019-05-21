# -*- coding: utf-8 -*-
import sys, traceback
import json
import logging
from misc2.helpers import ContractHelper, ConfigMap
from time import sleep
import time, datetime
from os import listdir
from os.path import isfile, join
from threading import Lock
from comms.ib_heartbeat import IbHeartBeat
import threading, urllib2
from optparse import OptionParser
import finopt.options_data as options_data
from kafka import KafkaProducer
from misc2.observer import Publisher
from ib.opt import ibConnection
import importlib
import copy
## 
## to run, start kafka server on vsu-01 <administrator, password>
## start ibgw or tws
## edit subscript list
## check the settings in the console
## 

class IbKafkaProducer(Publisher):
    
    IB_TICK_PRICE = 'tickPrice'
    IB_TICK_SIZE = 'tickSize'
    IB_TICK_OPTION_COMPUTATION = 'tickOptionComputation'
    
    EVENT_READ_FILE_LINE = 'event_read_file_line'
    PUBLISH_EVENTS = [EVENT_READ_FILE_LINE, IB_TICK_PRICE, IB_TICK_SIZE, IB_TICK_OPTION_COMPUTATION]
    DEFAULT_CONFIG = {
      'name': 'ib_mds',
      'group_id': 'mds',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'order_transmit': False
    }    
    def __init__(self, config):
        
        Publisher.__init__(self, IbKafkaProducer.PUBLISH_EVENTS)
        

        
        
        temp_kwargs = copy.copy(config)
        self.config = copy.copy(IbKafkaProducer.DEFAULT_CONFIG)
        for key in self.config:
            if key in temp_kwargs:
                self.config[key] = temp_kwargs.pop(key)        
        self.config.update(temp_kwargs)            
        

        self.tlock = Lock()
        self.persist = {}
        self.quit = False
        self.toggle = False
        self.id2contract = {'conId': 1, 'id2contracts': {} }
        kafka_host = self.config["kafka.host"]
        kafka_port =  self.config["kafka.port"]
        self.persist['is_persist'] = self.config["ib_mds.is_persist"]
        self.persist['persist_dir'] =self.config["ib_mds.persist_dir"]
        self.persist['file_exist'] = False
        self.persist['spill_over_limit'] = int(self.config["ib_mds.spill_over_limit"])
        #self.load_processors(config['ib_mds.processors'])       
        IbKafkaProducer.IB_TICK_PRICE = self.config["kafka.ib.topic.tick_price"]
        IbKafkaProducer.IB_TICK_SIZE = self.config["kafka.ib.topic.tick_size"]
        logging.info('******* Starting IbKafkaProducer')
        logging.info('IbKafkaProducer: connecting to kafka host: %s...' % kafka_host)
        logging.info('IbKafkaProducer: message mode is async')



        self.producer = KafkaProducer(bootstrap_servers='%s:%s' % (kafka_host, kafka_port))
        try:
            replay = True if self.config['replay_dir'] <> None else False
        except:
            replay = False 
        if not replay:
            
            self.start_ib_connection()
            self.load_tickers() 
        else:
            self.replay(self.config['replay_dir'])
        
#from cep.ib_mds.processor.us_stkopt import StockOptionsSnapshot
        self.main_loop()
    
    def load_processors(self, plist):
        
        
        def instantiate_processor(p):
            p_toks = p.split('.')
            class_name = p_toks[len(p_toks)-1]
            module_name = p_toks[:len(p_toks)-1]
            module = importlib.import_module(p.replace('.%s' % class_name, ''))
            class_ = getattr(module, class_name)
            return class_(self.config)
                
        processors = map(instantiate_processor, plist)
        for p in processors:
            map(lambda e: self.register(e, p, getattr(p, e)), IbKafkaProducer.PUBLISH_EVENTS)
        
        
        
    
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
                self.producer.send('my.price', msg.encode('utf-8'))
                tick_id+=1

            sleep(sec)
        
        
    def add_contract(self, clist):
        tuple = map(lambda x: x if x not in [''] else None, clist) 
        c = options_data.ContractHelper.makeContract(tuple)
        self.con.reqMktData(self.id2contract['conId'], c, '', False)
        self.id2contract['id2contracts'][self.id2contract['conId']] = options_data.ContractHelper.makeRedisKeyEx(c)
        self.id2contract['conId']+=1
        
    def delete_contract(self, id):
        pass
        
    def on_ib_conn_broken(self, msg):
        logging.error('IbKafkaProducer: connection is broken!')
        self.ib_conn_status = 'ERROR'
        self.tlock.acquire() # this function may get called multiple times
        try:                 # block until another party finishes executing
            if self.ib_conn_status == 'OK': # check status
                return                      # if already fixed up while waiting, return 
            
            self.con.eDisconnect()
    
            
            host = self.config["ib_mds.gateway"]
            port = int(self.config["ib_mds.ib_port"])
            appid = int(self.config["ib_mds.appid.id"])        
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
#                 a = AlertHelper(self.config)
#                 a.post_msg('ib_mds recovered from broken ib conn, re-subscribe tickers...')            
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

        
        if msg.typeName in ['tickPrice', 'tickSize', 'tickOptionComputation']:

            t = create_tick_kmessage(msg)             
            logging.debug(t)   
            if self.toggle:
                print t
            self.producer.send(IbKafkaProducer.IB_TICK_PRICE if msg.typeName == 'tickPrice' else IbKafkaProducer.IB_TICK_SIZE, t) 
            if self.persist['is_persist']:
                self.write_message_to_file(t)
                
            event = msg.typeName
            attrs = dir(msg)
            params = dict()
            for e in filter(lambda a: a not in ['typeName', 'keys', 'items', 'values'] and '__' not in a, attrs):
                params[e] = getattr(msg, e)
            params['contract_key'] = self.id2contract['id2contracts'][msg.tickerId]
            
            self.dispatch(event, params)
            
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
        host = self.config["ib_mds.gateway"]
        port = int(self.config["ib_mds.ib_port"])
        appid = int(self.config["ib_mds.appid.id"])          
        self.con = ibConnection(host, port, appid)
        self.con.registerAll(self.on_ib_message)
        rc = self.con.connect()
        if rc:
            self.ib_conn_status = 'OK'
        logging.info('start_ib_connection: connection status to IB is %d' % rc)
        
        # start heart beat monitor
        self.ibh = IbHeartBeat(self.config)
        self.ibh.register_listener([self.on_ib_conn_broken])
        self.ibh.run()        
        
    '''
        SAMPLE FILE FORMAT
        
        #
        # US Stock options
        # symbol, sectype, exch, ccy, expiry, strike, right
        #
        #
        GOOG,STK,SMART,USD,,,
        GOOG,OPT,SMART,USD,20190510,1172.5,C
        GOOG,OPT,SMART,USD,20190510,1172.5,P
        GOOG,OPT,SMART,USD,20190510,1175,C
        GOOG,OPT,SMART,USD,20190510,1175,P
        GOOG,OPT,SMART,USD,20190510,1177.5,C
        GOOG,OPT,SMART,USD,20190510,1177.5,P
        GOOG,OPT,SMART,USD,20190510,1180,C
        GOOG,OPT,SMART,USD,20190510,1180,P
        GOOG,OPT,SMART,USD,20190510,1182.5,C
        GOOG,OPT,SMART,USD,20190510,1182.5,P
    
    '''
    def load_tickers(self, path=None):
        
        self.id2contract = {'conId': 1, 'id2contracts': {} }
        
        
        if path is None:
            path = self.config["ib_mds.subscription.fileloc"]
        logging.info('load_tickers: attempt to open file %s' % path)
        fr = open(path)
        for l in fr.readlines():
            if l[0] not in ['#', '', ' ']:
                 
                self.add_contract([t for t in l.strip('\n').split(',')])
            
            self.dispatch(IbKafkaProducer.EVENT_READ_FILE_LINE, {'record': l})

    def do_work(self):
        while not self.quit:
            sleep(1)
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
                interval = (msg_ts - (last_record_ts if last_record_ts <> None else msg_ts)).microseconds / 500000.0   #100000.0
                
                print '%s %s %s' % (msg_ts.strftime('%Y-%m-%d %H:%M:%S.%f'), s_msg, fn)
                self.producer.send(IbKafkaProducer.IB_TICK_PRICE if msg['typeName'] == 'tickPrice' else IbKafkaProducer.IB_TICK_SIZE, s_msg)
                 
                last_record_ts = msg_ts
                sleep(interval)

        files = sorted([ join(dir_loc,f) for f in listdir(dir_loc) if (isfile(join(dir_loc,f)) and f.endswith('.txt')) ])   
                 
        for f in files:
            process_msg(f)
        




    def main_loop(self):
        def print_menu():
            menu = {}
            menu['1']="list all subscribed contracts," 
            menu['2']="turn/on off  output"
            menu['3']="Start up configuration"
            menu['4']=""
            menu['9']="Exit"
    
            choices=menu.keys()
            choices.sort()
            for entry in choices: 
                print entry, menu[entry]                             
            
        def get_user_input(selection):
                
                print_menu()
                while 1:
                    resp = sys.stdin.readline()
                    response[0] = resp.strip('\n')        
        try:
            
            response = [None]
            user_input_th = threading.Thread(target=get_user_input, args=(response,))
            user_input_th.daemon = True
            user_input_th.start()               
            self.pcounter = 0
            self.menu_loop_done = False
            while not self.menu_loop_done: 
                
                sleep(.5)
                
                if response[0] is not None:
                    selection = response[0]
                    if selection =='1':
                        print ''.join('%s: %s\n' % (k, v) for k, v in self.id2contract['id2contracts'].iteritems())
                    elif selection == 't':
                        self.toggle = False if self.toggle else True
                    elif selection == '9': 
                        print 'quit command received...'
                        self.quit = True                        
                        sys.exit(0)
                        break
                    else: 
                        pass                        
                    response[0] = None
                    print_menu()                
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('ib_mds: caught user interrupt. Shutting down...')
                sys.exit(0)   
            

if __name__ == '__main__':
           
    
    parser = OptionParser()

    parser.add_option("-r", "--replay",
                      dest="replay_dir",
                      help="replay recorded mds files stored in the specified directory")
    parser.add_option("-s", "--symbols",
                      action="store", dest="symbols_file", 
                      help="the file defines stocks symbols for which to save ticks")                      
    parser.add_option("-f", "--config_file",
                      action="store", dest="config_file", 
                      help="path to the config file")                

    
    options, arguments = parser.parse_args()

    #print options, arguments
    
    if len(sys.argv) < 2:
        print("Usage: %s [options] <config file>" % sys.argv[0])
        exit(-1)    

    
    kwargs = ConfigMap().kwargs_from_file(options.config_file)
    for option, value in options.__dict__.iteritems():
        
        if value <> None:
            kwargs[option] = value

    #cfg_path= options.config_file

    if options.symbols_file:
        kwargs['ib_mds.subscription.fileloc']= options.symbols_file
      
    logconfig = kwargs['ib_mds.logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        

    logging.info('config settings: %s' % kwargs)
    #replay = True if options.replay_dir <> None else False 
    ik = IbKafkaProducer(kwargs)
    
#     if not replay:
#         ik.load_tickers()    
#     else:
#         ik.replay(options.replay_dir)
        
#    ik.run_forever()
    