#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2010  Nathanael C. Fritz
    This file is part of SleekXMPP.
    See the file LICENSE for copying permission.
"""
import sys, traceback
import json
import logging
import ConfigParser
from time import sleep
import time, datetime
import sleekxmpp
import redis
from threading import Lock
from redisQueue import RedisQueue

import threading

# Python versions before 3.0 do not use UTF-8 encoding
# by default. To ensure that Unicode is handled properly
# throughout SleekXMPP, we will set the default encoding
# ourselves to UTF-8.
if sys.version_info < (3, 0):
    from sleekxmpp.util.misc_ops import setdefaultencoding
    setdefaultencoding('utf8')
else:
    raw_input = input




class AlertMsgBot(sleekxmpp.ClientXMPP):

    """
    A basic SleekXMPP bot that will log in, send a message,
    and then log out.
    """
    config = None
    rsq= None

    recipients = None
    quit = False
    tlock = None
    

    def __init__(self, config):
        

        self.config = config
        host = config.get("redis", "redis.server").strip('"').strip("'")
        port = config.get("redis", "redis.port")
        db = config.get("redis", "redis.db")
        qname = config.get("alert_bot", "msg_bot.redis_mq").strip('"').strip("'")
        qprefix = config.get("alert_bot", "msg_bot.redis_prefix").strip('"').strip("'")
        
        self.rsq = RedisQueue(qname, qprefix, host, port, db)
        logging.info('Connect to redis on server->%s:%s db->%s  qname->%s:%s' % (host, port, db, qprefix, qname))
        jid = config.get("alert_bot", "msg_bot.jid").strip('"').strip("'")
        password = config.get("alert_bot", "msg_bot.pass").strip('"').strip("'")

        sleekxmpp.ClientXMPP.__init__(self, jid, password)
        
        self.recipients = eval(config.get("alert_bot", "msg_bot.recipients").strip('"').strip("'"))
        self.tlock = Lock()

        # The session_start event will be triggered when
        # the bot establishes its connection with the server
        # and the XML streams are ready for use. We want to
        # listen for this event so that we we can initialize
        # our roster.
        self.add_event_handler("session_start", self.start, threaded=True)

    def start(self, event):
        """
        Process the session_start event.
        Typical actions for the session_start event are
        requesting the roster and broadcasting an initial
        presence stanza.
        Arguments:
            event -- An empty dictionary. The session_start
                     event does not provide any additional
                     data.
        """
        self.send_presence()
        self.get_roster()

        self.rsq.put("Greetings! The alert bot has just started!")
        t = threading.Thread(target = self.process_alerts(), args=())
        t.start()
#         self.send_message(mto=self.recipient,
#                           mbody=self.msg,
#                           mtype='chat')

        
        
        
    def process_alerts(self):
        self.quit = False
        while self.quit <> True:
            if not self.rsq.empty():
                msg = self.rsq.get()
                logging.debug('process_alerts: received msg: {%s}' % msg)
                self.send_msg(msg)
            sleep(1)

        # Using wait=True ensures that the send queue will be
        # emptied before ending the session.
        self.disconnect(wait=True)


    def send_msg(self, msg):
        self.tlock.acquire()
        try:
            for r in self.recipients:
                self.send_message(r, msg, mtype='chat')            
        finally:
            self.tlock.release()  
        
        
#
# alert helper doesn't care whether the xmpp server 
# has started or not, all it cares is being able to put the message 
# into redisQueue
class AlertHelper():
    q = None
    
    def __init__(self, config, rhost=None, rport=None, rdb=None, chatq=None, prefix=None):
        if config:
            rhost = config.get("redis", "redis.server").strip('"').strip("'")
            rport = config.get("redis", "redis.port")
            rdb = config.get("redis", "redis.db")
            chatq = config.get("alert_bot", "msg_bot.redis_mq").strip('"').strip("'")
            prefix = config.get("alert_bot", "msg_bot.redis_prefix").strip('"').strip("'")
        
            
            
        self.q = RedisQueue(chatq, prefix, rhost, rport, rdb)
        
    def post_msg(self, msg):
        self.q.put(msg)      
        
    def flush_all(self):
        i = 0
        while not self.q.empty():
            self.q.get()
            i+=1
        return i
        

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: %s <config file>" % sys.argv[0])
        exit(-1)    

    cfg_path= sys.argv[1:]
    config = ConfigParser.SafeConfigParser()
    if len(config.read(cfg_path)) == 0: 
        raise ValueError, "Failed to open config file" 
      
    logconfig = eval(config.get("alert_bot", "msg_bot.logconfig").strip('"').strip("'"))
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(**logconfig)

    xmpp = AlertMsgBot(config)
    xmpp.register_plugin('xep_0030') # Service Discovery
    xmpp.register_plugin('xep_0199') # XMPP Ping

    if xmpp.connect(): #('192.168.1.1', 5222), True, True, False):
        xmpp.process(block=False)
        logging.info('Complete initialization...Bot will now run forever')
        
        # two different ways to instantiate alerthelper
        cfg = {'rhost':'localhost', 'rport':6379, 'rdb': 3, 'chatq': 'chatq', 'prefix': 'alert_bot'}
        a = AlertHelper(None, *cfg )
        #a = AlertHelper(config)
        
        i = a.flush_all()
        a.post_msg('from AlertHelper: flushed %d old messages.' % i)
        
    else:
        print("Unable to connect.")