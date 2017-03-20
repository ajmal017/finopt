import json
import sys
import logging

class NotImplementedException(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)


class Subscriber:
    def __init__(self, name):
        self.name = name

    def update(self, event, param=None):
        #print('{} got message "{}"'.format(self.name, message))
        raise NotImplementedException('update function is not implemented! Override the function by subclassing Subscriber!')
        
class Publisher:
    def __init__(self, events):
        # maps event names to subscribers
        # str -> dict
        self.events = { event : dict()
                          for event in events }
    
    def get_subscribers(self, event):
        return self.events[event]
    
    def register(self, event, who, callback=None):
        if callback == None:
            callback = getattr(who, 'update')
        self.get_subscribers(event)[who] = callback
    
    def unregister(self, event, who):
        del self.get_subscribers(event)[who]
    
    def dispatch(self, event, params=None):
        
        for subscriber, callback in self.get_subscribers(event).items():
            callback(event, **params)
            #print 'observer:: subscriber**** %s' % subscriber
#             try:
#                 callback(event, **params)
#             except TypeError:
#                 logging.error (sys.exc_info()[0])
            
#############################################################
# Test classes to demo usage of Publisher and Subscriber
#
#
class Producer(Publisher):
    def __init__(self, events):
        Publisher.__init__(self, events)
    

class Consumer(Subscriber):
    def __init__(self, name):
        Subscriber.__init__(self, name)
        
    def update(self, event, param=None):
        print('override %s: %s %s %s' % (self.name, event, "<empy param>" if not param else param,
                                         
                                         '<none>' if not param else param.__class__))

    def trigger(self, event, param=None):
        print('trigger %s: %s %s %s' % (self.name, event, "<empy param>" if not param else param,
                                         
                                         '<none>' if not param else param.__class__))
        

if __name__ == '__main__':


    
    p = Producer(['e1', 'e2'])
    bb = Consumer('bb')
    cc = Consumer('cc')

    p.register('e1', bb)
    p.register('e1', cc, cc.trigger)
    p.register('e2', cc)
    p.dispatch('e1', {'xx':123, 'yy':444})
    p.dispatch('e2', str({'x':123, 'y':444}))
    
    
    
    