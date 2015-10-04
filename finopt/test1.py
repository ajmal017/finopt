# -*- coding: utf-8 -*-
import threading, logging, time

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("vsu-01:9092")
        producer = SimpleProducer(client)#, async=True)

        while True:
            producer.send_messages('my.topic', "this is a test")
            producer.send_messages('my.topic', "\xc2Hola, bravo!")

            
            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("vsu-01:9092")
        consumer = SimpleConsumer(client, "test-group", "my.price")

        for message in consumer:
            
            print(message)

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    #time.sleep(5)
    while 1:
        pass
    

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()