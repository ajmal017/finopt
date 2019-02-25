 #!/usr/bin/env python
import threading, logging, time, traceback
import sys
import copy
import datetime
import uuid
from Queue import Queue
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from misc2.observer import NotImplementedException
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from kafka.errors import NoBrokersAvailable
# 
# packages required for ConsumerNextIteratorPersist
import json
from redis import Redis

from misc2.observer import Subscriber, Publisher
from numpy.distutils.fcompiler import none
from types import NoneType




class Producer(threading.Thread):
    daemon = True

    def run(self):
        try:
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            topics = ['my_topic', 'my_topic2']
            i = 0
            while True:
                #today = datetime.date.today()
                s = "%d %s test %s" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
                logging.info(s)
                producer.send(topics[i%2], s)
                
                time.sleep(.45)
                i=i+1
        except NoBrokersAvailable:
            logging.error("NoBrokersAvailable: Has kafka started?")


class BaseProducer(threading.Thread, Subscriber):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  verbose=verbose)
        """
        kwargs:
            bootstrap_host
            bootstrap_host
            redis_host
            session_timeout_ms: 
        """
        
        
        self.name = '%s-%s' % (name, uuid.uuid5(uuid.NAMESPACE_OID, name)) 
        logging.info('BaseProducer __init__: name=%s' % self.name)
        self.args = args
        self.kwargs = kwargs
        
        self.event_q = Queue()
        return


    def send_message(self, topic, plain_text):
        self.event_q.put((topic, plain_text))
        self.event_q.task_done()

    def set_stop(self):
        self.done = True
        
    def run(self):
        try:
            producer = KafkaProducer(bootstrap_servers='%s:%s' % (self.kwargs['bootstrap_host'], self.kwargs['bootstrap_port']))
            
            self.done = False
            while self.done <> True:
                #today = datetime.date.today()
                
                #if not self.event_q.empty():
                while not self.event_q.empty():
                    topic, plain_text = self.event_q.get()
                    #s = "BaseProducer topic:[%s] msg:[%s]" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
                    logging.info("BaseProducer topic:[%s] msg:[%s]" % (topic, plain_text))
                    producer.send(topic, plain_text)
                    
                # to prevent excessive CPU use
                time.sleep(0.1)
            
            
            logging.info ('******** BaseProducer exit done.')
            producer.close(1)
                
        except NoBrokersAvailable:
            logging.error("NoBrokersAvailable: Has kafka started?")
    

class BaseConsumer(threading.Thread, Publisher):
    
    #KB_EVENT = "on_kb_event"
    SLOW_CONSUMER_CHECK_EVERY = 50
    SLOW_CONSUMER_QUALIFY_NUM = 500
    KB_REACHED_LAST_OFFSET = "on_kb_reached_last_offset"
    
    #my_topics =  {'my_topic':{}, 'my_topic2':{}}    

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  verbose=verbose)
        """
        kwargs:
            bootstrap_host
            bootstrap_host
            redis_host
            redis_port
            redis_db
            group_id
            consumer_id: name 
            topics- a list of topic strings
            session_timeout_ms 
            consumer_timeout_ms
            seek_to_end- a list of topics that only wants the latest message
        """
        
        
        self.name = '%s-%s' % (name, uuid.uuid5(uuid.NAMESPACE_OID, name)) 
        logging.info('BaseConsumer __init__: name=%s' % self.name)
        self.args = args
        #self.kwargs = kwargs
        self.kwargs = copy.copy(kwargs)  
        self.rs = Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        try:
            self.kwargs['seek_to_end']
        except KeyError:
            self.kwargs['seek_to_end'] = []
        self.my_topics = {}
        for t in self.kwargs['topics']:
            self.my_topics[t]= {} 
            
        #self.events = {event: dict() for event in [BaseConsumer.KB_EVENT, BaseConsumer.KB_REACHED_LAST_OFFSET]}
        self.events = {event: dict() for event in [BaseConsumer.KB_REACHED_LAST_OFFSET] + self.kwargs['topics']}
        return
    
    

    
    """
     each consumer has its own set of topics offsets stored in redis
     for consumer A and consumer B (with different group_ids) subscribing to the same my_topic, each 
     each of them will need to keep track of its own offsets
     to make the redis key unique by consumer, the key is created by topic + '@' + consumer name
     example: my_topic@consumerA
     
     offsets = { topic-consumer_name:
                     {
                         partition0: offset0,
                         partition1: offset1,... 
                     }
               }
    """
    
    def consumer_topic(self, tp):
        return tp + '@' + self.name 
    
    
    def clear_offsets(self):
        """
            clear the offsets in redis by removing the value from redis
            and emptying the internal my_topics dict
            
            clear offsets is ncessary when the offset in redis was saved 
            at a time since kafka manager was shut down
            when kafka restarts, previously buffered 
            messages are no longer available and instead it will restart its offset at 0.
            Reading an old offset by BaseConsumer will cause it to think that it
            is still receving old buffered messages from Kafa but in fact all the messages 
            since the last shut down of kafka are all gone
        """
        for t in self.kwargs['topics']:
            self.my_topics[t]= {}
            logging.info("BaseConsumer:clear_offsets Deleting %s from redis..." % self.consumer_topic(t))
            self.rs.delete(self.consumer_topic(t))
            
             
    
    
    def persist_offsets(self, topic, partition, offset):
        #self.rs.set(self.consumer_topic(topic), json.dumps({'partition': partition, 'offset':offset}))
        self.my_topics[topic][str(partition)] = offset
        self.rs.set(self.consumer_topic(topic), json.dumps(self.my_topics[topic]))
    
        
    def extract_message_content(self, message):
        logging.info('BaseConsumer: extract_message_content. %s %s' % (type(message), message))
        try:
            return json.loads(message.value)
        except ValueError:
            logging.info('extract_message_content exception: %s' % message)
            return {}
    
    def set_stop(self):
        self.done = True
    
    def run(self):
        logging.info('BaseConsumer:run. %s:%s started' % (self.kwargs['group_id'], self.name))
        
        if self.kwargs['clear_offsets'] == True:
            self.clear_offsets()
        
        consumer = KafkaConsumer(bootstrap_servers='%s:%s' % (self.kwargs['bootstrap_host'], self.kwargs['bootstrap_port']),
                                 auto_offset_reset='earliest',
                                 #
                                 # consumers having the same group id works as   
                                 # a group to seize messages published by the publisher
                                 # (like a queue, each message is consumed exactly once
                                 #  by a consumer)
                                 #
                                 #
                                 # in a 'pub-sub' environment, set each consumer having 
                                 # a unique group_id
                                 #
                                 group_id = self.kwargs['group_id'],
                                 client_id = self.name,
                                 #
                                 # session_timeout_ms is the time it takes for another consumer 
                                 # that has the same group_id to pick up the work
                                 # to consume messages when this consumer is dead 
                                 # 
                                 session_timeout_ms = self.kwargs['session_timeout_ms'],
                                 #
                                 # 
                                 #
                                 partition_assignment_strategy=[RoundRobinPartitionAssignor],
                                 #
                                 #
                                 consumer_timeout_ms=self.kwargs['consumer_timeout_ms']
                                )


        
        for topic in self.my_topics.keys():
            # 
            # if a topic offset is stored previously (the consumer was run before), 
            # then load the offset values
            # and save it locally into the my_topics map 
            # else self.my_topics[topic] would have zero elements in it
            if self.rs.keys(self.consumer_topic(topic)):
                self.my_topics[topic] = json.loads(self.rs.get(self.consumer_topic(topic)))

        logging.info('BaseConsumer:run. Topics subscribed: %s' % self.my_topics)
        
            
        consumer.subscribe(self.my_topics.keys())

        #consumer.seek_to_end(TopicPartition(topic='my_topic', partition=0))

        self.done = False
       
            
        while self.done <> True:
            try:
                message = consumer.next()
                

                # the next if block is there to serve information purpose only
                # it may be useful to detect slow consumer situation
                if message.offset % BaseConsumer.SLOW_CONSUMER_QUALIFY_NUM == 0:
                    highwater = consumer.highwater(TopicPartition(message.topic, message.partition))
                    logging.info( "BaseConsumer [%s]:highwater:%d offset:%d part:%d <%s>" %  (self.name, highwater, message.offset, message.partition, message.topic))
                    
                    if highwater - message.offset >= BaseConsumer.SLOW_CONSUMER_QUALIFY_NUM:
                        logging.warn("BaseConsumer:run Slow consumer detected! current: %d, highwater:%d, gap:%d" %
                                        (message.offset, highwater, highwater - message.offset))
                # the next block is designed to handle the first time the
                # consumer encounters a topic partition it hasnt' seen yet
                try:
                    # the try catch block ensures that on the first encounter of a topic/partition
                    # the except block is executed once and only once, thereafter since the 
                    # dictionary object keys are assigned the exception will never
                    # be caught again
                    # 
                    # the try catch is supposed to be faster than a if-else block...
                    # 
                    # the below statement has no meaning, it is there merely to ensure that 
                    # the block fails on the first run
                    self.my_topics[message.topic][str(message.partition)] 
                except KeyError:

                    highwater = consumer.highwater(TopicPartition(message.topic, message.partition))
                    gap = highwater - message.offset
                    logging.info( "*** On first iteration: [Topic:%s:Part:%d:Offset:%d]: Number of messages lagging behind= %d. Highwater:%d" 
                                  % (message.topic, message.partition, message.offset, gap, highwater))
                                                                                                
                    for t, ps in map(lambda t: (t, consumer.partitions_for_topic(t)), self.my_topics.keys()):
                        try:
                            logging.info ("*** On first iteration: T/P Table: topic:[%s] %s" % (t.rjust(25),  
                                                             ','.join('part:%d, off:%d' % (p, consumer.position(TopicPartition(topic=t, partition=p))) for p in ps)
                                                             ))
                        except TypeError:
                            logging.warn ('*** On first iteration: [*** %s not registered in kafka topics yet ***]. This message should go away the next time the program is run.' % t)
                            continue
                        
                    self.persist_offsets(message.topic, message.partition, message.offset)
                    self.my_topics[message.topic] = json.loads(self.rs.get(self.consumer_topic(message.topic)))
                        
                        
                    if '*' in self.kwargs['seek_to_end'] or message.topic in self.kwargs['seek_to_end']:
                        #print 'baseconsumer run %s %d' % (message.topic, gap)
                        # if there is no gap
                        '''
                        
                            use seek_to_end only for messages that keep streaming and you don't
                            care whether messages are lost or not
                            
                        
                        '''
                        if gap <=1:
                            # the message is valid for dispatching and not to be skipped
                            self.dispatch(message.topic, self.extract_message_content(message))
                            logging.debug('*** On first iteration: Gap=%d Dispatch this valid message to the listener <%s>' % (gap, message.value))
                        else: # gap exists
                            logging.info("*** On first iteration: [Topic:%s:Part:%d:Offset:%d]: Gap:%d Attempting to seek to latest message ..." 
                                         % (message.topic, message.partition, message.offset, gap))                            
                            consumer.seek_to_end((TopicPartition(topic=message.topic, partition= message.partition)))
                            # skip this message from dispatching to listeners
                            continue
                
                    
                """
                    the message.value received from kafaproducer is expected to contain 
                    plain text encoded as a json string
                    the content of message.value is not altered. it's content is stored in a dict object 
                    with key = 'value' and enriched with additional kafa metadata
                                    
                     
                    it is the subscriber's job to interpret the content stored in the 'value' key. Typically
                    it means decoding the content by invoking json.loads 
                      
                """
                if self.my_topics[message.topic][str(message.partition)] > message.offset:
                    if (self.my_topics[message.topic][str(message.partition)] - message.offset) % 1000 == 0: 
                        logging.info('BaseConsumer ********************** old message...discarding %s %d(%d)' % (message.topic, message.offset, 
                                                                                        self.my_topics[message.topic][str(message.partition)]))
                else:
                    #if self.my_topics[message.topic][str(message.partition)] == message.offset:
                    # if the stored offset in redis equals to the current offset
                    # notify the observers
                    # the "and" condition ensures that on a fresh start of kafka server this event is not triggered as
                    # both saved value in redis and current offset are both 0
                    if self.my_topics[message.topic][str(message.partition)] == message.offset and message.offset <> 0:
                        self.dispatch(BaseConsumer.KB_REACHED_LAST_OFFSET, self.extract_message_content(message))
                        self.dispatch(message.topic, self.extract_message_content(message))
                        logging.info('********************** reached the last message previously processed %s %d' % (message.topic, message.offset))
                    else:
                        self.persist_offsets(message.topic, message.partition, message.offset)
                        self.dispatch(message.topic, self.extract_message_content(message))
                        
            except StopIteration:
                logging.debug('BaseConsumer:run StopIteration Caught. No new message arriving...')
                continue
            except TypeError:
                logging.error('BaseConsumer:run. Caught TypeError Exception while processing a message. Malformat json string? %s: %s' % (message.topic, message.value))
                logging.error(traceback.format_exc())
                
        consumer.close()   
        logging.info ('******** BaseConsumer exit done.')



class BaseMessageListener(Subscriber):
    
    def __init__(self, name):
        self.name = name
    
    
    def update(self, event, param=none):
        try:
            event_fn = getattr(self, event)
            event_fn(param)
        except AttributeError:
            err_msg = 'BaseMessageListener:update| function %s not implemented.' % event
            logging.error('BaseMessageListener [%s]:update %s' % (self.name, err_msg))
        logging.debug("BaseMessageListener [%s]:update|Event type:[%s] content:[%s]" % (self.name, event, json.dumps(param) if param <> None else "<empty param>"))



class Prosumer(BaseProducer):
    # wrapper object
    PROSUMER_DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-prosumer' ,
        'group_id': 'kafka-prosumer-default-group',
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_max_wait_ms': 500,
        'fetch_min_bytes': 1,
        'max_partition_fetch_bytes': 1 * 1024 * 1024,
        'request_timeout_ms': 40 * 1000,
        'retry_backoff_ms': 100,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'default_offset_commit_callback': lambda offsets, response: True,
        'check_crcs': True,
        'metadata_max_age_ms': 5 * 60 * 1000,
        'partition_assignment_strategy': (RoundRobinPartitionAssignor),
        'heartbeat_interval_ms': 3000,
        'session_timeout_ms': 30000,
        'max_poll_records': sys.maxsize,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'consumer_timeout_ms': 1000,
        'connections_max_idle_ms': 9 * 60 * 1000, # not implemented yet
    }    
    
    
    def __init__(self, name, kwargs=None):
        
        
        self.kwargs = copy.copy(self.PROSUMER_DEFAULT_CONFIG)
        for key in self.kwargs:
            if key in kwargs:
                self.kwargs[key] = kwargs.pop(key)        
        self.kwargs.update(kwargs)

        logging.info('\nProsumer:init: **** Configurations dump ***')
        logging.info('\n'.join('%s:%s' % (k.ljust(40), self.kwargs[k]) for k in sorted(self.kwargs)))

        BaseProducer.__init__(self, group=None, target=None, name=name,
                 args=(), kwargs=self.kwargs, verbose=None)

        
        self.kconsumer = BaseConsumer(name=name,  kwargs=self.kwargs)
        
        
    
    def add_listener_topics(self, listener, topics):
        try:
            map(lambda e: self.kconsumer.register(e, listener, getattr(listener, e)), topics)
        except AttributeError as ex:
            logging.error("Prosumer:add_listener_topics. Function not implemented in the listener. %s" % ex)
            raise NotImplementedException
            
        
    def add_listeners(self, listeners):
        
        for l in listeners:
            map(lambda e: self.kconsumer.register(e, l, getattr(l, e)), self.kwargs['topics'])
        
    

    def is_stopped(self):
        return self.stopped
    
    def set_stop(self):
        BaseProducer.set_stop(self)
        self.kconsumer.set_stop()
        logging.info('Prosumer:set_stop. Pending kconsumer to shutdown in 2s...')
        self.stopped = True
    
    def start_prosumer(self):
        self.stopped = False
        self.kconsumer.start()
        self.start()
        
    
    def message_loads(self, text_msg):
        return json.loads(text_msg)
    
    
    def message_dumps(self, obj_msg):
        return json.dumps(obj_msg)



class SimpleMessageListener(BaseMessageListener):
    
    def __init__(self, name):
        BaseMessageListener.__init__(self, name)
        self.cnt_my_topic = 0
        self.cnt_my_topic2 = 0
    
#     def on_kb_event(self, param):
#         print "on_kb_event [%s] %s" % (self.name, param)
    def my_topic(self, e, param):
        if self.cnt_my_topic % 50 == 0:
            print "SimpleMessageListener:my_topic. %s" % param
            self.cnt_my_topic += 1

    def my_topic2(self, e, param):
        if self.cnt_my_topic2 % 50 == 0:
            print "SimpleMessageListener:my_topic2. %s" % param
            self.cnt_my_topic2 += 1

        
    def on_kb_reached_last_offset(self, param):
        print "on_kb_reached_last_offset [%s] %s" % (self.name, param)

    
class Prosumer2Listener(BaseMessageListener):
    '''
    test code used by test cases
    '''
    
    def __init__(self, name, producer):
        BaseMessageListener.__init__(self, name)
        self.producer = producer
        self.i = 0
    


    def findTemperature(self, event, location):
        
        logging.info("[%s] received findTemperature:[%s]" % (self.name, location))
        #vars= self.producer.message_loads(items['value'])
        
        import urllib2
        response = urllib2.urlopen('http://rss.weather.gov.hk/rss/SeveralDaysWeatherForecast.xml')
        self.producer.send_message('temperature', self.producer.message_dumps({'current': "%s" % (response.read())}))
        #self.producer.send_message('temperature', self.producer.message_dumps({'current': "%s" % (time.strftime("%b %d %Y %H:%M:%S"))}))
        self.producer.send_message('temperatureEnd', self.producer.message_dumps({'empty': None}))
        #id': self.i, 'reqid': vars['reqid'], 'response' : "%s" % (time.strftime("%b %d %Y %H:%M:%S"))})
                                   
        self.i = self.i + 1
        
    def temperature(self, event, current):
        logging.info("[%s] received event [%s] content:[%s]" % (self.name, event, current))
        
    
    def temperatureEnd(self, event, empty):
        logging.info("[%s] received event [%s] content:[%s]" % (self.name, event, empty))
        self.producer.set_stop()
    
        
        
            
def test_prosumer2(mode, boot_host):
    
    bootstrap_host = boot_host
    
    if mode == 'A':
                
        topicsA = ['temperature', 'temperatureEnd']
        
        pA = Prosumer(name='A', kwargs={'bootstrap_host':bootstrap_host, 'bootstrap_port':9092,
                                        'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                        'group_id': 'groupA', 'session_timeout_ms':10000,
                                                 'topics': topicsA, 'clear_offsets' : True,
                                                 'seek_to_end': [topicsA],
                                                 })
        sA = Prosumer2Listener('earA', pA)
        
        pA.add_listeners([sA])
        pA.start_prosumer()
        i = 0

        try:
            pA.send_message('findTemperature', pA.message_dumps({'location':'HKG'}))
            while not pA.is_stopped(): #i < 5:
                
                #pA.send_message('gw_req_subscriptions', pA.message_dumps({'desc': 'requesting subscription msg counter:%d' % i, 
                #                                                    'reqid': i}))
                i= i + 1
                time.sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                pA.set_stop()
                pA.join()
      
        print ('end...exiting')
        

        
    else:    
        topicsB = ['findTemperature']
        
        pB = Prosumer(name='B', kwargs={'bootstrap_host':bootstrap_host, 'bootstrap_port':9092,
                                        'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                        'group_id': 'groupB', 'session_timeout_ms':10000,
                                                 'topics': topicsB, 'clear_offsets' : True,
                                                 'seek_to_end': [topicsB],
                                                 })
        sB = Prosumer2Listener('earB', pB)
        pB.add_listeners([sB])
        pB.start_prosumer()
        try:
            
            while not pB.is_stopped(): #i < 5:
                
                
                time.sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                pB.set_stop()
                pB.join()    
    
    
    

class TestProducer(BaseProducer):
    pass

def test_base_proconsumer(mode, bootstrap_host):
    '''
        This example demonstrates
        
        1) use of consumer_timeout_ms to break out from the consumer.next loop
        2) how to trap ctrl-c and break out of the running threads
        3) using Queue to store calls to producer.send_message
        4) using redis to store the consumer last processed offsets
        5) use of try-catch block to implement seek_to_latest offset
        6) inherit and implement MessageListener to subscribe messages dispatched by the consumer 
    '''
    topics = ['my_topic', 'my_topic2']
    if mode == 'P':
        #Producer().start()
        
        tp = TestProducer(name = 'testproducer', kwargs={
                                             'bootstrap_host':bootstrap_host, 'bootstrap_port':9092,
                                             'topics': topics})
        tp.start()
        i = 0 
        while True:
            
            #today = datetime.date.today()
            try:
                s = "%d %s test %s" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
                logging.info(s)
                
                '''
                    send_message requires 2 function arguments
                    argument 1 is the name of the message function expected to be fired in the consumer upon
                    receiving the message sent by send_message
                    argument 2 is a json parameter string with a list of key value pairs
                    the message function in the consumer is expected to have all the keys declared in the 
                    function parameter list
                    
                    for this example the consumer uses a SimpleMessageListener, thus in the SimpleMessageListener
                    it must declare two topic functions namely my_topic and my_topic2
                    
                    to register the SimpleMessageListner to receive callback events, call the register function of 
                    consumer which takes 3 parameters: the event name, the listener reference, and the call back function
                    
                    the standard callback function takes event_name as its 2nd parameter, 
                    follow by a list of parameters (self, event_name, param1, param2...)
                    
                            def my_topic(self, event, param):
                            def my_topic2(self, event, param):
                            
                            
                
                '''
                tp.send_message(topics[i%2], json.dumps({'param': s}))
                
                time.sleep(.5)
                i=i+1
            except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                tp.set_stop()
                tp.join()
                sys.exit(-1)
                    
        
    else:
        
        bc = BaseConsumer(name='bc', kwargs={'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                             'bootstrap_host':'vsu-bison', 'bootstrap_port':9092,
                                             'group_id':'gid', 'session_timeout_ms':10000, 'topics': topics,
                                             'clear_offsets': True, 'consumer_timeout_ms':1000,
                                             # uncomment the next line to process messages from since the program was last shut down
                                             # if seek_to_end is present, for the topic specified the consumer will begin
                                             # sending the latest message to the listener
                                             # note that the list only specifies my_topic to receive the latest
                                             # but not my_topic2. Observe the different behavior by checking the message offset 
                                             # in the program log
                                             'seek_to_end': ['my_topic'],            
                                             }) 
        #bml = BaseMessageListener('bml')
        sml = SimpleMessageListener('simple')
        bc.register(BaseConsumer.KB_REACHED_LAST_OFFSET, sml)
        
        '''
            tell bc to dispatch my_topic event to the call back function
            sml.my_topic of listener sml
            another way to reference the function name in sml is to use the getattr 
            function in python: getattr(obj, 'obj_func_name')
            
        '''
        bc.register('my_topic', sml, sml.my_topic)
        bc.register('my_topic2', sml, sml.my_topic2)
        bc.start()
        while True:
            
            try:
                time.sleep(.25)
            except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                bc.set_stop()
                bc.join()
                sys.exit(-1)



    
def main():
    
    #
    # test cases
    #
    tp = [ test_base_proconsumer, test_prosumer2]
    
    if len(sys.argv) != 4:
        print("Usage: %s <role(producer or consumer): P|C> <test case #[0..1]> <kafka_bootstrap_host name/ip>" % sys.argv[0])
        print "\n".join('case #%d: %s' % (i, tp[i].__name__) for i in range(len(tp)))
        print "example: python %s P 1" % sys.argv[0]
        print "example: python %s C 1" % sys.argv[0]
        exit(-1)    


    mode = sys.argv[1] 
    #gid = sys.argv[2] if sys.argv[2] <> None else "q-group"  


    '''
        program start up parameter controls which function to test
    
        tp[0] -> run test_base_proconsumer
        tp[1] -> run test_prosumer2
        
        
        test_prosumer2
        
        Provide weather information: tp[1]('B') ex. ./base_messaging.sh B 1 vorsprung
        Request weather information: tp[1]('A') ex. ./base_messaging.sh A 1 vorsprung
    
        
        
    '''    
    tp[int(sys.argv[2])](mode, sys.argv[3])

    #time.sleep(30)
#     while 1:
#         try:
#             time.sleep(5)
#             pass
#         except (KeyboardInterrupt, SystemExit):
#                 logging.error('caught user interrupt')
#                 sys.exit(-1)
 
    
    

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()