 #!/usr/bin/env python
import threading, logging, time
import sys
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



class Producer(threading.Thread):
    daemon = True

    def run(self):
        try:
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            topics = ['my-topic', 'my-topic2']
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
                
                if not self.event_q.empty():
                    topic, plain_text = self.event_q.get()
                    #s = "BaseProducer topic:[%s] msg:[%s]" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
                    logging.info("BaseProducer topic:[%s] msg:[%s]" % (topic, plain_text))
                    producer.send(topic, plain_text)
                    
                # to prevent excessive CPU use
                time.sleep(0.1)
            
            logging.info('completed run')
            
                
        except NoBrokersAvailable:
            logging.error("NoBrokersAvailable: Has kafka started?")
    

class BaseConsumer(threading.Thread, Publisher):
    
    #KB_EVENT = "on_kb_event"
    KB_REACHED_LAST_OFFSET = "on_kb_reached_last_offset"
    
    #my_topics =  {'my-topic':{}, 'my-topic2':{}}    

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
            topics: a list of topic strings
            session_timeout_ms: 
            consumer_timeout_ms
        """
        
        
        self.name = '%s-%s' % (name, uuid.uuid5(uuid.NAMESPACE_OID, name)) 
        logging.info('BaseConsumer __init__: name=%s' % self.name)
        self.args = args
        self.kwargs = kwargs
        self.rs = Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        self.my_topics = {}
        for t in self.kwargs['topics']:
            self.my_topics[t]= {} 
            
        #self.events = {event: dict() for event in [BaseConsumer.KB_EVENT, BaseConsumer.KB_REACHED_LAST_OFFSET]}
        self.events = {event: dict() for event in [BaseConsumer.KB_REACHED_LAST_OFFSET] + self.kwargs['topics']}
        return
    
    
    # no use: doesn't work
    def seek_to_last_read_offset(self, consumer):
        for t in self.my_topics.keys():
            po = json.loads(self.rs.get(t))
            consumer.seek(TopicPartition(topic=t, partition=po['partition']), po['offset'])
    
    """
     each consumer has its own set of topics offsets stored in redis
     for consumer A and consumer B (with different group_ids) subscribing to the same my-topic, each 
     each of them will need to keep track of its own offsets
     to make the redis key unique by consumer, the key is created by topic + '@' + consumer name
     example: my-topic@consumerA
     
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
            
             
        #raise NotImplementedException
    
    
    def persist_offsets(self, topic, partition, offset):
        #self.rs.set(self.consumer_topic(topic), json.dumps({'partition': partition, 'offset':offset}))
        self.my_topics[topic][str(partition)] = offset
        self.rs.set(self.consumer_topic(topic), json.dumps(self.my_topics[topic]))
    
    def enrich_message(self, message):
        return {'value': message.value, 'partition':message.partition, 'offset': message.offset}
    
    def set_stop(self):
        self.done = True
    
    def run(self):
        print '%s:%s started' % (self.kwargs['group_id'], self.name)
        
        if self.kwargs['clear_offsets'] == 1:
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
                                 consumer_timeout_ms=1000
                                )


        
        for topic in self.my_topics.keys():
            # 
            # if a topic offset is stored previously (the consumer was run before), 
            # then load the offset values
            # and save it locally into the my_topics map 
            # else self.my_topics[topic] would have zero elements in it
            if self.rs.keys(self.consumer_topic(topic)):
                self.my_topics[topic] = json.loads(self.rs.get(self.consumer_topic(topic)))

        print self.my_topics
        
            
        consumer.subscribe(self.my_topics.keys())

        
        #consumer.seek_to_end(TopicPartition(topic='my-topic', partition=0))

        self.done = False
        while self.done <> True:
            try:
                message = consumer.next()
                
                #time.sleep(0.25)
                if message.offset % 50 == 0:
                    logging.info( "[%s]:highwater:%d offset:%d part:%d <%s>" % (self.name, consumer.highwater(TopicPartition(message.topic, message.partition)),
                                                                             message.offset, message.partition, message.value))
                
    #             for t, ps in map(lambda t: (t, consumer.partitions_for_topic(t)), self.my_topics.keys()):
    #                 print "t:%s %s" % (t,  ','.join('p:%d, offset:%d' % (p, consumer.position(TopicPartition(topic=t, partition=p))) for p in ps)) # consumer.position(TopicPartition(topic=t, partition=p)))
                
                # if this is the first time the consumer is run
                # it contains no offsets in the redis map, so it has 
                # 0 elements in the map,
                # then insert a new offset in redis and populate
                # the local my_topics dict
                
                if len(self.my_topics[message.topic]) == 0:
                    self.persist_offsets(message.topic, message.partition, message.offset)
                    self.my_topics[message.topic] = json.loads(self.rs.get(self.consumer_topic(message.topic)))
                    #continue
                    
                """
                    the message.value received from kafaproducer is expected to contain 
                    plain text encoded as a json string
                    the content of message.value is not altered. it's content is stored in a dict object 
                    with key = 'value' along with additional kafa metadata
                                    
                     
                    it is the subscriber's job to interpret the content stored in the 'value' key. Typically
                    it means decoding the content by invoking json.loads 
                      
                """
                if self.my_topics[message.topic][str(message.partition)] > message.offset:
                    print '********************** old message...discarding %s %d' % (message.topic, message.offset)
                else:
                    #if self.my_topics[message.topic][str(message.partition)] == message.offset:
                    # if the stored offset in redis equals to the current offset
                    # notify the observers
                    # the "and" condition ensures that on a fresh start of kafka server this event is not triggered as
                    # both saved value in redis and current offset are both 0
                    if self.my_topics[message.topic][str(message.partition)] == message.offset and message.offset <> 0:
                        self.dispatch(BaseConsumer.KB_REACHED_LAST_OFFSET, self.enrich_message(message))
                        logging.info('********************** reached the last message previously processed %s %d' % (message.topic, message.offset))
                    else:
                        self.persist_offsets(message.topic, message.partition, message.offset)
                        #self.dispatch(BaseConsumer.KB_EVENT, {'message': message})
                        self.dispatch(message.topic, self.enrich_message(message))
            except StopIteration:
                logging.debug('BaseConsumer:run StopIteration Caught. No new message arriving...')
                continue
            
            
        logging.info ('**********************************************done')



class BaseMessageListener(Subscriber):
    
    
    
    def update(self, event, param=none):
        try:
            event_fn = getattr(self, event)
            event_fn(param)
        except AttributeError:
            err_msg = 'BaseMessageListener:update| function %s not implemented.' % event
            logging.error('BaseMessageListener [%s]:update %s' % (self.name, err_msg))
        logging.debug("BaseMessageListener [%s]:update|Event type:[%s] content:[%s]" % (self.name, event, json.dumps(param) if param <> None else "<empty param>"))


class SimpleMessageListener(BaseMessageListener):
    
    def __init__(self, name):
        BaseMessageListener.__init__(self, name)
    
#     def on_kb_event(self, param):
#         print "on_kb_event [%s] %s" % (self.name, param)
    
    def on_kb_reached_last_offset(self, param):
        print "on_kb_reached_last_offset [%s] %s" % (self.name, param)


class Prosumer(BaseProducer):
    # wrapper object
    def __init__(self, name, kwargs=None):
        BaseProducer.__init__(self, group=None, target=None, name=name,
                 args=(), kwargs=kwargs, verbose=None)
        self.kconsumer = BaseConsumer(name=name,  kwargs=kwargs)
        self.kwargs = kwargs
        
    
    def add_listener_topics(self, listener, topics):
        map(lambda e: self.kconsumer.register(e, listener, getattr(listener, e)), topics)
        
    def add_listeners(self, listeners):
        
        for l in listeners:
            map(lambda e: self.kconsumer.register(e, l, getattr(l, e)), self.kwargs['topics'])
        
    

    
    def set_stop(self):
        BaseProducer.set_stop(self)
        self.kconsumer.set_stop()
    
    def start_prosumer(self):
        self.kconsumer.start()
        self.start()
        
    
    def message_loads(self, text_msg):
        return json.loads(text_msg)
    
    
    def message_dumps(self, obj_msg):
        return json.dumps(obj_msg)

    
class SubscriptionListener(BaseMessageListener):
    
    
    def __init__(self, name, producer):
        BaseMessageListener.__init__(self, name)
        self.producer = producer
        self.i = 0
    
    def gw_subscription_changed(self, event, items):
        logging.info("[%s] received gw_subscription_changed content: [%s]" % (self.name, items))
        #print 'SubscriptionListener:gw_subscription_changed %s' % items
        
#     def on_kb_event(self, param):
#         print "on_kb_event [%s] %s" % (self.name, param)
    def gw_req_subscriptions(self, event, items):
        
        logging.info("[%s] received gw_req_subscriptions content:[%s]" % (self.name, items))
        vars= self.producer.message_loads(items['value'])
        self.producer.send_message('gw_subscription_changed', self.producer.message_dumps({'id': self.i, 'reqid': vars['reqid'], 
                                                                          'response' : "%s" % (time.strftime("%b %d %Y %H:%M:%S"))})
                                   )
        self.i = self.i + 1
        
    def reqMktData(self, event, items):
        logging.info("[%s] received %s content:[%s]" % (self.name, event, items))
        self.producer.send_message('tickPrice', 
                        self.producer.message_dumps({'field':4, 'typeName':'tickPrice', 'price':1.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':79, 'canAutoExecute':0}))
        
    
    def tickPrice(self, event, items):   
        logging.info("[%s] received %s content:[%s]" % (self.name, event, items))
        
    def on_kb_reached_last_offset(self, event, items):
        logging.info("[%s] received on_kb_reached_last_offset content: [%s]" % (self.name, items))
        print "on_kb_reached_last_offset [%s] %s" % (self.name, items)
        
            
def test_prosumer2(mode):
    
    if mode == 'A':
                
        topicsA = ['gw_subscription_changed', 'tickPrice']
        
        pA = Prosumer(name='A', kwargs={'bootstrap_host':'localhost', 'bootstrap_port':9092,
                                        'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                        'group_id': 'groupA', 'session_timeout_ms':10000,
                                                 'topics': topicsA, 'clear_offsets' : 0})
        sA = SubscriptionListener('earA', pA)
        
        pA.add_listeners([sA])
        pA.start_prosumer()
        i = 0

        try:
            pA.send_message('reqMktData', pA.message_dumps({'contract':'dummy'}))
            while True: #i < 5:
                
                #pA.send_message('gw_req_subscriptions', pA.message_dumps({'desc': 'requesting subscription msg counter:%d' % i, 
                #                                                    'reqid': i}))
                i= i + 1
                time.sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                pA.set_stop()
                pA.join()
      
            
        

        
    else:    
        topicsB = ['gw_req_subscriptions', 'reqMktData']
        
        pB = Prosumer(name='B', kwargs={'bootstrap_host':'localhost', 'bootstrap_port':9092,
                                        'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                        'group_id': 'groupB', 'session_timeout_ms':10000,
                                                 'topics': topicsB, 'clear_offsets' : 0})
        sB = SubscriptionListener('earB', pB)
        pB.add_listeners([sB])
        pB.start_prosumer()
        try:
            
            while True: #i < 5:
                
                pB.send_message('tickPrice', 
                        pB.message_dumps({'field':5, 'typeName':'tickPrice', 'price':2.0682, 'ts':1485661437.83, 'source':'IB', 'tickerId':79, 'canAutoExecute':0}))
                
                time.sleep(.45)
                
        except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                pB.set_stop()
                pB.join()    
    
    

    

class TestProducer(BaseProducer):
    pass

def test_base_proconsumer(mode):

    if mode == 'P':
        #Producer().start()
        topics = ['my-topic', 'my-topic2']
        tp = TestProducer(name = 'testproducer', kwargs={
                                             'bootstrap_host':'localhost', 'bootstrap_port':9092,
                                             'topics': topics})
        tp.start()
        i = 0 
        while True:
            
            #today = datetime.date.today()
            s = "%d %s test %s" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
            logging.info(s)
            tp.send_message(topics[i%2], s)
            
            time.sleep(.45)
            i=i+1
        
        
    else:
        
        bc = BaseConsumer(name='bc', kwargs={'redis_host':'localhost', 'redis_port':6379, 'redis_db':0,
                                             'bootstrap_host':'localhost', 'bootstrap_port':9092,
                                             'group_id':'gid', 'session_timeout_ms':10000, 'topics': ['my-topic', 'my-topic2']})
        #bml = BaseMessageListener('bml')
        sml = SimpleMessageListener('simple')
        #bc.register(BaseConsumer.KB_EVENT, bml)
        #bc.register(BaseConsumer.KB_REACHED_LAST_OFFSET, bml)
        bc.register(BaseConsumer.KB_EVENT, sml)
        bc.register(BaseConsumer.KB_REACHED_LAST_OFFSET, sml)
        
        bc.start()



    
def main():
    
    #
    # test cases
    #
    tp = [ test_base_proconsumer, test_prosumer2]
    
    if len(sys.argv) != 3:
        print("Usage: %s <role(producer or consumer): P|C> <test case #[0..1]>" % sys.argv[0])
        print "\n".join('case #%d: %s' % (i, tp[i].__name__) for i in range(len(tp)))
        print "example: python %s P 1" % sys.argv[0]
        print "example: python %s C 1" % sys.argv[0]
        exit(-1)    

    mode = sys.argv[1] 
    #gid = sys.argv[2] if sys.argv[2] <> None else "q-group"  

    
    tp[int(sys.argv[2])](mode)

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