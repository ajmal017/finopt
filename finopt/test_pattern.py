from misc2.helpers import ContractHelper

class Subscriber:
    def __init__(self, name):
        self.name = name
    def update(self, message):
        print('{} got message "{}"'.format(self.name, message))
        
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
    def dispatch(self, event, message):
        for subscriber, callback in self.get_subscribers(event).items():
            callback(message)
            
            
class SomeAbstraction( object ):
    xx = None
    def __init__(self):
        self.xx = 20
    

class Mixin1( object ):
    def something( self ):
        print 'mixin1' # one implementation

class Mixin2( object ):
    def something( self ):
        pass # another

class Concrete1( SomeAbstraction, Mixin1 ):
    pass

class Producer(Publisher):
    def __init__(self, events):
        Publisher.__init__(self, events)
        print self.events

class Consumer(Subscriber, Mixin1):
    def __init__(self, name):
        Subscriber.__init__(self, name)
    def update(self, message):
        print('override %s' % message)


def test_contracthelper():
    contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
     
    c1 = ContractHelper.makeContract(contractTuple)  
    contractTuple = ('QQQ', 'STK', 'SMART', 'USD', '', 0, '')
    c2 = ContractHelper.makeContract(contractTuple)
    
    print ContractHelper.is_equal(c1, c2)

    c2 = contractTuple = ('XQQQ', 'STK', 'SMART', 'USD', '', 0, '')
    print ContractHelper.is_equal(c1, c2)
    
if __name__ == '__main__':

    sa = SomeAbstraction()
    mx = Mixin1()
    c = Concrete1()#sa, mx)
    c.something()
    print c.xx
    
    p = Producer(['e1', 'e2'])
    p.get_subscribers('e1')
    bb = Consumer('bb')

    p.register('e1', bb)
    p.get_subscribers('e1')
    p.dispatch('e1', "e1 meessage!")
    bb.something()
    
    test_contracthelper()
    