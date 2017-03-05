#!/usr/bin/env python
import threading, logging, time
import signal
import sys
import datetime
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

# 
# packages required for ConsumerNextIteratorPersist
import json
from redis import Redis

class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        topics = ['my-topic', 'my-topic2']
        i = 0
        while True:
            #today = datetime.date.today()
            s = "%d %s test %s" % (i, topics[i%2], time.strftime("%b %d %Y %H:%M:%S"))
            logging.info(s)
            producer.send(topics[i%2], s)
            
            time.sleep(.15)
            i=i+1



class Consumer(threading.Thread):
    daemon = True
    my_topics =  ['my-topic', 'my-topic2']

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  verbose=verbose)
        self.name = name
        self.args = args
        self.kwargs = kwargs
        return
        
    def run(self):
        print '%s:%s started' % (self.kwargs['group_id'], self.name)
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='latest',
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
                                 session_timeout_ms = 10000,
                                 #
                                 # 
                                 #
                                 partition_assignment_strategy=[RoundRobinPartitionAssignor])

        consumer.subscribe(self.my_topics)
#         print consumer.partitions_for_topic("my-topic")
#         print consumer.assignment()
#         print consumer.subscription()
        
        #consumer.seek_to_end(TopicPartition(topic='my-topic', partition=0))

        
        for message in consumer:
            #time.sleep(0.25)
            logging.info( "%s:offset:%d part:%d %s" % (self.name, message.offset, message.partition, message.value))
            for t, ps in map(lambda t: (t, consumer.partitions_for_topic(t)), self.my_topics):
                print "t:%s %s" % (t,  ','.join('p:%d, offset:%d' % (p, consumer.position(TopicPartition(topic=t, partition=p))) for p in ps)) # consumer.position(TopicPartition(topic=t, partition=p)))
                      
            

        logging.info ('**********************************************done')



class ConsumerNextIteratorPersist(threading.Thread):
    # this class demonstrates the use of next iterator to process message
    # and logic to save offsets to an external storage2
    
    daemon = True
    my_topics =  {'my-topic':{}, 'my-topic2':{}}
     
    rs = None

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  verbose=verbose)
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.rs = Redis('localhost', 6379, 0)
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
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
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
                                 session_timeout_ms = 10000,
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
                
            #if self.my_topics[message.topic]['offset'] >= message.offset and self.my_topics[message.topic]['partition'] == message.partition:
            if self.my_topics[message.topic][str(message.partition)] >= message.offset:
                print '********************** processed previously...discarding %s %d' % (message.topic, message.offset)
            else:
                self.persist_offsets(message.topic, message.partition, message.offset)
            
        logging.info ('**********************************************done')


def test_queue_mechanism(mode, gid):
    #
    # at the console, start a producer by typing python test_kafka.py P x 1
    # the 2nd param is consumer group which has no use for a producer
    # (so just specify some jibberish for the program to run)
    # the 3rd param is the position of the test function in the test cases list
    #
    # create a new console, start a consumer by typing python test_kafka.py C group1
    # create another instance of the consumer, again issuing the command
    # python test_kafka.py C group1
    #
    # Make sure the producer has the following settings when it is first created:
    # partition_assignment_strategy=[RoundRobinPartitionAssignor])
    #
    # watch the 2 instances consuming messages simultaneously
    # kill one of the consumer and view the other one picks up message topic originally
    # assigned to the now deceased consumer instance
    # 
    # the time it takes for the 2nd consumer to assume the role of the first consumer 
    # is determined by the variable session_timeout_ms = 10000,
    threads = []
    if mode == 'P':
        threads.append(Producer())
    else:    
        threads = [
            Consumer(name='c1', kwargs={'group_id':gid}),
            Consumer(name='c2', kwargs={'group_id':gid})
            ]

    for t in threads:
        t.start()   

def test_recovery(mode, gid):
    #
    # this demo requires redis library to 
    # persist the topic offsets in the redis database
    #
    # 1. start a producer by typing test_kafka.py P x 1
    # 2. start a consumer and let it run for awhile
    # 3. kill the consumer but leave the producer running
    # 4. write down the last saved topic offset in redis
    # 5. after awhile, restart the consumer 
    # 6. notice the output of the consumer, there should 
    # be some output that says messages are discarded because
    # they had been processed previously 
    # 
    # the consumer self.name has to be the same across 
    # re-runs in order for the recovery to work
    # in this example, it has been hard coded to 'c_test_recovery'
    #
    threads = []
    if mode == 'P':
        threads.append(Producer())
    else:
        
        threads = [
            ConsumerNextIteratorPersist(name='c_test_recovery', kwargs={'group_id':gid}),
            #Consumer(name='c2')
            ]

    for t in threads:
        t.start()


def test_recovery_discard_aged():
    # internal pub/sub callback pattern
    #fire_event(f_msg, kv, age_factor)
    pass
    
def main():
    
    #
    # test cases
    #
    tp = [test_queue_mechanism, test_recovery, test_recovery_discard_aged]
    
    if len(sys.argv) != 4:
        print("Usage: %s <role(producer or consumer): P|C> <consumer group id> <test case #[0..1]>" % sys.argv[0])
        print "\n".join('case #%d: %s' % (i, tp[i].__name__) for i in range(len(tp)))
        print "example: python %s producer g2 1" % sys.argv[0]
        print "example: python %s consumer g2 1" % sys.argv[0]
        exit(-1)    

    mode = sys.argv[1] 
    gid = sys.argv[2] if sys.argv[2] <> None else "q-group"  

    
    tp[int(sys.argv[3])](mode, gid)

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