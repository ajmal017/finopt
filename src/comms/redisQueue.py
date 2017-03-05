import redis
import sys

class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace, host, port, db):
        self.__db= redis.Redis(host, port, db)
        self.key = '%s:%s' %(namespace, name)
        
        try:
            self.__db.client_list()
        except redis.ConnectionError:
            print "RedisQueue failed to connect to the server. "
        
        #print host, port, db, self.key

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    def peek(self, pos=0):
        return self.__db.lindex(self.key, pos)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)
    
    
