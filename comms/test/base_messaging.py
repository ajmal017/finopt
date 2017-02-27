#!/usr/bin/env python
import threading, logging, time
import sys
import datetime
import uuid
from Queue import Queue
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

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
        logging.info('BaseConsumer __init__: name=%s' % self.name)
        self.args = args
        self.kwargs = kwargs
        
        self.event_q = Queue()
        return

    def send_message(self, topic, message):
        self.event_q.put((topic, message))
        self.event_q.task_done()

    def run(self):
        try:
            producer = KafkaProducer(bootstrap_servers='%s:%s' % (self.kwargs['bootstrap_host'], self.kwargs['bootstrap_port']))
            
            
            while True:
                #today = datetime.date.today()
                
                if not self.event_q.empty():
                    topic, message = self.event_q.get()
                    #s = "BaseProducer topic:[%s] msg:[%s]" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
                    logging.debug("BaseProducer topic:[%s] msg:[%s]" % (topic, message))
                    producer.send(topic, message)
                
        except NoBrokersAvailable:
            logging.error("NoBrokersAvailable: Has kafka started?")
    

class BaseConsumer(threading.Thread, Publisher):
    
    KB_EVENT = "on_kb_event"
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
        """
        
        
        self.name = '%s-%s' % (name, uuid.uuid5(uuid.NAMESPACE_OID, name)) 
        logging.info('BaseConsumer __init__: name=%s' % self.name)
        self.args = args
        self.kwargs = kwargs
        self.rs = Redis(self.kwargs['redis_host'], self.kwargs['redis_port'], self.kwargs['redis_db'])
        self.my_topics = {}
        for t in self.kwargs['topics']:
            self.my_topics[t]= {} 
            
        self.events = {event: dict() for event in [BaseConsumer.KB_EVENT, BaseConsumer.KB_REACHED_LAST_OFFSET]}
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
    
    def persist_offsets(self, topic, partition, offset):
        #self.rs.set(self.consumer_topic(topic), json.dumps({'partition': partition, 'offset':offset}))
        self.my_topics[topic][str(partition)] = offset
        self.rs.set(self.consumer_topic(topic), json.dumps(self.my_topics[topic]))
        
    def run(self):
        print '%s:%s started' % (self.kwargs['group_id'], self.name)
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
                                 partition_assignment_strategy=[RoundRobinPartitionAssignor])



        
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

        done = False
        while not done:
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
                continue
                
            
            if self.my_topics[message.topic][str(message.partition)] > message.offset:
                print '********************** old message...discarding %s %d' % (message.topic, message.offset)
            else:
                if self.my_topics[message.topic][str(message.partition)] == message.offset:
                    self.dispatch(BaseConsumer.KB_REACHED_LAST_OFFSET, {'message': message})
                    logging.info('********************** reached the last message previously processed %s %d' % (message.topic, message.offset))
                else:
                    self.persist_offsets(message.topic, message.partition, message.offset)
                    self.dispatch(BaseConsumer.KB_EVENT, {'message': message})
            
            
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
    
    def on_kb_event(self, param):
        print "on_kb_event [%s] %s" % (self.name, param)
    
    def on_kb_reached_last_offset(self, param):
        print "on_kb_reached_last_offset [%s] %s" % (self.name, param)


class Prosumer(BaseProducer):
    pass


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
    tp = [ test_base_proconsumer]
    
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
    while 1:
        try:
            time.sleep(5)
            pass
        except (KeyboardInterrupt, SystemExit):
                logging.error('caught user interrupt')
                sys.exit(-1)

    
    

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()