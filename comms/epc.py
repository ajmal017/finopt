#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import logging
import ConfigParser
from time import sleep
import time, datetime
import sleekxmpp
from threading import Lock
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
import threading


        
        
class ExternalProcessComm(threading.Thread):
    
    producer = None
    consumer = None
    def __init__(self, config):

        super(ExternalProcessComm, self).__init__()
        host = config.get("epc", "kafka.host").strip('"').strip("'")
        port = config.get("epc", "kafka.port")

        client = KafkaClient('%s:%s' % (host, port))
        self.producer = SimpleProducer(client, async=False)
        self.consumer = SimpleConsumer(client, "epc.group", "epc.topic")
                
    def post_msg(self, topic, msg):
        self.producer.send_messages(topic, msg)


    def run(self):
        for message in self.consumer:
            
            logging.info(message)




if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0: 
        raise ValueError, "Failed to open config file" 
      
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

    e = ExternalProcessComm(config)
    e.start()
    e.post_msg('epc.topic', 'test msg')