#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import logging
import ConfigParser
from time import sleep
import time, datetime
from threading import Lock
from kafka.client import KafkaClient
from kafka import KafkaConsumer
from kafka.producer import SimpleProducer
from kafka.common import LeaderNotAvailableError, ConsumerTimeout
import threading

from misc2.helpers import ContractHelper
import uuid


from comms.tws_protocol_helper import TWS_Protocol

class AnalyticsListener(threading.Thread):
    """ This class is used to receive kafka
        events broadcasted by OptionCalculationEngine 

    """    
    consumer = None
    command_handler = None
    stop_consumer = False
    
    reg_all_callbacks = []



    def __init__(self, host, port, id=None):

        super(AnalyticsListener, self).__init__()
        client = KafkaClient('%s:%s' % (host, port))
        self.producer = SimpleProducer(client, async=False)
        

 
        # consumer_timeout_ms must be set - this allows the consumer an interval to exit from its blocking loop
        self.consumer = KafkaConsumer( *[(v,0) for v in list(TWS_Protocol.oceEvents)] , \
                                       metadata_broker_list=['%s:%s' % (host, port)],\
                                       client_id = str(uuid.uuid1()) if id == None else id,\
                                       group_id = 'epc.group',\
                                       auto_commit_enable=True,\
                                       consumer_timeout_ms = 2000,\
                                       auto_commit_interval_ms=30 * 1000,\
                                       auto_offset_reset='largest') # discard old ones
        
        self.reset_message_offset()
        
        
        
        

    def reset_message_offset(self):
        # 90 is a magic number or don't care (max_num_offsets)
        topic_offsets =  map(lambda topic: (topic, self.consumer.get_partition_offsets(topic, 0, -1, 90)), TWS_Protocol.oceEvents)
        topic_offsets = filter(lambda x: x <> None, map(lambda x: (x[0], x[1][1], max(x[1][0], 0)) if len(x[1]) > 1 else None, topic_offsets))
        logging.info("AnalyticsListener: topic offset dump ------:")
        logging.info (topic_offsets)
        logging.info('AnalyticsListener set topic offset to the latest point\n%s' % (''.join('%s,%s,%s\n' % (x[0], x[1], x[2]) for x in topic_offsets)))
        
        # the set_topic_partitions call clears out all previous settings when starts
        # therefore it's not possible to do something like this:
        # self.consumer.set_topic_partitions(('gw_subscriptions', 0, 114,)
        # self.consumer.set_topic_partitions(('tickPrice', 0, 27270,))
        # as the second call will wipe out whatever was done previously
        self.consumer.set_topic_partitions(*topic_offsets)        
        

                           
    def run(self):

 
            # keep running until someone tells us to stop
            while self.stop_consumer == False:
            #while True:
  
                    try:
                        # the next() function runs an infinite blocking loop
                        # it will raise a consumertimeout if no message is received after a pre-set interval   
                        message = self.consumer.next()
                        
                        
                        logging.debug("AnalyticsListener: %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                     message.offset, message.key,
                                                     message.value))
                        
                        [f(message.value) for f in self.reg_all_callbacks]                        
                        
                        
                    except ConsumerTimeout:
                        logging.info('AnalyticsListener run: ConsumerTimeout. Check new message in the next round...')

                                        
    def stop(self):
        logging.info('AnalyticsListener: --------------- stopping consumer')
        self.stop_consumer = True
        

    def registerAll(self, funcs):
        [self.reg_all_callbacks.append(f) for f in funcs]
        
                