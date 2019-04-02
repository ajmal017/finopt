# -*- coding: utf-8 -*-

import sys, traceback
import json
import logging
import ConfigParser
from ib.opt import ibConnection
from time import sleep
import datetime
import threading
from optparse import OptionParser
from misc2.helpers import ConfigMap

class IbHeartBeat():
    config = None
    quit = False
    prev_state = ''
    q = None
    #chat_handle = None
    last_broken_time = None
    alert_callbacks = []
    
    def __init__(self, kwargs):
        self.kwargs = kwargs
        
        # ensure the message will get printed right away when the connection is first broken
        self.last_broken_time =  datetime.datetime.now() - datetime.timedelta(seconds=90)  
        
        

    def register_listener(self, fns):
        self.alert_callbacks = [fn for fn in fns]
        
    def alert_listeners(self, msg):
        [fn(msg) for fn in self.alert_callbacks]

    def run(self):
        t = threading.Thread(target = self.keep_trying, args=())
        t.start()

    def shutdown(self):
        self.quit = True
        logging.info('ib_hearbeat: attempting to shutdown....')
        
    def keep_trying(self):
        host = self.kwargs["ib_heartbeat.gateway"]
        port = self.kwargs["ib_heartbeat.ib_port"]
        appid = self.kwargs["ib_heartbeat.appid.id"]      
        try_interval = self.kwargs["ib_heartbeat.try_interval"]
        suppress_msg_interval = self.kwargs["ib_heartbeat.suppress_msg_interval"]
        logging.info('ib gateway->%s:%d, appid->%d, try_interval->%d, suppress msg interval->%d' % \
                     (host, port, appid, try_interval, suppress_msg_interval))
        
        
        '''''
            this function breaks up long sleep into smaller intervals
            to allow a chance to process termination request
        '''''
        def smart_sleep(sleep_duration):
            num_steps = 20
            short_break = sleep_duration / num_steps
            for i in range(num_steps):
                if self.quit:
                    break
                else:
                    sleep(short_break)
        
        while not self.quit:
            con = ibConnection(host, port, appid)
            rc = con.connect()
            if rc:
                if self.prev_state == 'broken':
                    msg = '*** Connection restored at %s **********' % datetime.datetime.now().strftime('%H:%M:%S')
                    #self.chat_handle.post_msg(msg)
                    self.alert_listeners(msg)
                    self.prev_state = ''
                    # reset to a much earlier time
                    self.last_broken_time = datetime.datetime.now() - datetime.timedelta(seconds=90)
                con.eDisconnect()
            else:
                msg = '*** Connection to IB API is broken **********'
                now = datetime.datetime.now()
                
                #self.prev_state = 'broken' 
                #print now, self.last_broken_time, (now - self.last_broken_time).seconds
                if self.last_broken_time == None or (now - self.last_broken_time).seconds > suppress_msg_interval:
                    #self.chat_handle.post_msg(msg)
                    logging.error(msg)
                    self.alert_listeners(msg)
                    self.last_broken_time = now 
                    
               
            smart_sleep(try_interval)
            
        logging.info('ib_hearbeat: shut down complete...')


        
    

if __name__ == '__main__':
           
  
    
#     if len(sys.argv) != 2:
#         print("Usage: %s <config file>" % sys.argv[0])
#         exit(-1)    
# 
#     cfg_path= sys.argv[1:]
#     config = ConfigParser.SafeConfigParser()
#     if len(config.read(cfg_path)) == 0:      
#         raise ValueError, "Failed to open config file" 
#     
#       
#     logconfig = eval(config.get("ib_mds", "ib_mds.logconfig").strip('"').strip("'"))
#     logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
#     logging.basicConfig(**logconfig)    
#     ibh = IbHeartBeat(config)
#     
#     def warn_me(msg):
#         print 'warn_me: received %s' % msg
#     
#     ibh.register_listener([warn_me])
#     ibh.run()
    
    
    
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)   
    parser.add_option("-f", "--config_file",
                      action="store", dest="config_file", 
                      help="path to the config file")
    
    (options, args) = parser.parse_args()
    
    kwargs = ConfigMap().kwargs_from_file(options.config_file)
    for option, value in options.__dict__.iteritems():
        
        if value <> None:
            kwargs[option] = value


    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        

    ibh = IbHeartBeat(kwargs)
    
    def warn_me(msg):
        print 'warn_me: received %s' % msg
    
    ibh.register_listener([warn_me])
    ibh.run()
    
