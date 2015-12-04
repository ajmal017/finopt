import sys
from time import sleep, strftime
import time, datetime
import ConfigParser
from optparse import OptionParser
import logging
import thread
import threading
import traceback
import json
from threading import Lock
from ib.ext.Contract import Contract
from ib.ext.EWrapper import EWrapper
from ib.ext.EClientSocket import EClientSocket
from ib.ext.ExecutionFilter import ExecutionFilter
from ib.ext.Execution import Execution
from ib.ext.OrderState import OrderState
from ib.ext.Order import Order

from kafka.client import KafkaClient
from kafka import KafkaConsumer
from kafka.producer import SimpleProducer
from kafka.common import LeaderNotAvailableError

from misc2.helpers import ContractHelper, OrderHelper, ExecutionFilterHelper
from comms.ib_heartbeat import IbHeartBeat
from comms.tws_protocol_helper import TWS_Protocol 

class KTopicsManager:
    
    def __init__(self, host, port):
        client = KafkaClient('%s:%s' % (host, port))
        self.producer = SimpleProducer(client, async=False)
        self.cli_request_handler = KafkaConsumer( *[(v,0) for v in list(TWS_Protocol.topicMethods) + list(TWS_Protocol.gatewayMethods) ], \
                                   metadata_broker_list=['%s:%s' % (host, port)],\
                                   group_id = 'epc.tws_gateway',\
                                   auto_commit_enable=True,\
                                   auto_commit_interval_ms=30 * 1000,\
                                   auto_offset_reset='largest')        

    def register(self, topics):
        for t in topics:
            try:
                print ('registering %s' % t)
                self.producer.send_messages(t, 'register topic - %s' % t)
            except:
                continue


             
             
def register_topics(host, port):
    
        
    reg = KTopicsManager(host, port)
    reg.register(TWS_Protocol.topicEvents)
    reg.register(TWS_Protocol.topicMethods)    
    reg.register(TWS_Protocol.gatewayEvents)
    reg.register(TWS_Protocol.gatewayMethods)
    reg.register(TWS_Protocol.oceEvents)
    #reg.register(TWS_Protocol.oceMethods)
    
    
    

    
if __name__ == '__main__':    
          
    
     
       
    register_topics('vsu-01', 9092)
             