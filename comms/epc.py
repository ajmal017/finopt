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
from kafka import KafkaConsumer
from kafka.producer import SimpleProducer
from kafka.common import LeaderNotAvailableError
import threading

class EPCPub():
    
    producer = None


    
    def __init__(self, config):

        
        host = config.get("epc", "kafka.host").strip('"').strip("'")
        port = config.get("epc", "kafka.port")

        
        
        #client = KafkaClient('%s:%s' % (host, port))
        
        # this version of kafka client has already deprecated 
        # the program no longer works with the latest version of kafka
        # the next line is just to make the program starts up
        client = None
        
        self.producer = SimpleProducer(client, async=False)
        
                
    def post_msg(self, topic, msg):
        self.producer.send_messages(topic, msg)


    def post_portfolio_summary(self, dict):
        
        msg= (time.time(), ExternalProcessComm.EPC_TOPICS['EPC_PORT_SUMMARY_TOPIC'], dict)
        
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_PORT_SUMMARY_TOPIC'], json.dumps(msg))


    def post_portfolio_items(self, ldict):
        msg= (time.time(), ExternalProcessComm.EPC_TOPICS['EPC_PORT_ITEM_TOPIC'], ldict)
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_PORT_ITEM_TOPIC'], json.dumps(msg))
                             

    def post_account_summary(self, dict):
        msg = (time.time(), ExternalProcessComm.EPC_TOPICS['EPC_ACCT_SUMMARY_TOPIC'], dict)
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_ACCT_SUMMARY_TOPIC'], json.dumps(msg))

        
class ExternalProcessComm(threading.Thread):
    
    producer = None
    consumer = None
    EPC_TOPICS= {'EPC_PORT_SUMMARY_TOPIC': 'port_summary', 
                 'EPC_PORT_ITEM_TOPIC': 'port_item',
                 'EPC_ACCT_SUMMARY_TOPIC': 'acct_summary'}
    
    def __init__(self, config):

        super(ExternalProcessComm, self).__init__()
        host = config.get("epc", "kafka.host").strip('"').strip("'")
        port = config.get("epc", "kafka.port")

        client = KafkaClient('%s:%s' % (host, port))
        self.producer = SimpleProducer(client, async=False)
        #sleep(1)
        
        
        print 'create EPC'
        
        
        # the kafkaConsumer will fail with a no topic error if the topic is not found in the broker
        # the next line uses the producer to produce the required topic which will create one 
        # if it has not been created already
        
        [self.post_msg(v, 'init msg') for k,v in ExternalProcessComm.EPC_TOPICS.iteritems()] 
        self.consumer = KafkaConsumer( *[(v,0) for k,v in ExternalProcessComm.EPC_TOPICS.iteritems()], \
                                       metadata_broker_list=['%s:%s' % (host, port)],\
                                       group_id = 'epc.group',\
                                       auto_commit_enable=True,\
                                       auto_commit_interval_ms=30 * 1000,\
                                       auto_offset_reset='largest') # discard old ones
#         https://kafka.apache.org/08/configuration.html
#         What to do when there is no initial offset in Zookeeper or if an offset is out of range:
#         * smallest : automatically reset the offset to the smallest offset
#         * largest : automatically reset the offset to the largest offset
#         * anything else: throw exception to the consumer. If this is set to largest, 
#         the consumer may lose some messages when the number of partitions, for the topics 
#         it subscribes to, changes on the broker. To prevent data loss during partition addition, set auto.offset.reset to smallest
                
    def post_msg(self, topic, msg):
        self.producer.send_messages(topic, msg)


    def post_portfolio_summary(self, dict):
        msg= (time.time(), dict)
        
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_PORT_SUMMARY_TOPIC'], json.dumps(msg))


    def post_portfolio_items(self, ldict):
        msg= (time.time(), ldict)
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_PORT_ITEM_TOPIC'], json.dumps(msg))
        
    def post_account_summary(self, dict):
        msg = (time.time(), dict)
        self.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_ACCT_SUMMARY_TOPIC'], json.dumps(msg))
                             
    def run(self):
        
        for message in self.consumer:
            
            logging.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))

            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))



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
    
    e.post_msg(ExternalProcessComm.EPC_TOPICS['EPC_PORT_SUMMARY_TOPIC'], 'test msg')