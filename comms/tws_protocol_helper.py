#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

class TWS_Protocol:
    topicMethods = ('eConnect', 'reqAccountUpdates', 'reqOpenOrders', 'reqExecutions', 'reqIds', 'reqNewsBulletins', 'cancelNewsBulletins', \
                      'setServerLogLevel', 'reqAccountSummary', 'reqPositions', 'reqAutoOpenOrders', 'reqAllOpenOrders', 'reqManagedAccts',\
                       'requestFA', 'reqMktData', 'reqHistoricalData', 'placeOrder', 'eDisconnect')

    topicEvents = ('accountDownloadEnd', 'execDetailsEnd', 'updateAccountTime', 'deltaNeutralValidation', 'orderStatus',\
              'updateAccountValue', 'historicalData', 'openOrderEnd', 'updatePortfolio', 'managedAccounts', 'contractDetailsEnd',\
              'positionEnd', 'bondContractDetails', 'accountSummary', 'updateNewsBulletin', 'scannerParameters', \
              'tickString', 'accountSummaryEnd', 'scannerDataEnd', 'commissionReport', 'error', 'tickGeneric', 'tickPrice', \
              'nextValidId', 'openOrder', 'realtimeBar', 'contractDetails', 'execDetails', 'tickOptionComputation', \
              'updateMktDepth', 'scannerData', 'currentTime', 'error_0', 'error_1', 'tickSnapshotEnd', 'tickSize', \
              'receiveFA', 'connectionClosed', 'position', 'updateMktDepthL2', 'fundamentalData', 'tickEFP')
    
    
    
    gatewayMethods = ('gw_req_subscriptions',)
    gatewayEvents = ('gw_subscriptions',)
    
    oceMethods = ()
    oceEvents = ('optionAnalytics',)
    
    
    
    def json_loads_ascii(self, data):    
    
        def ascii_encode_dict(data):
            ascii_encode = lambda x: x.encode('ascii') if isinstance(x, unicode) else x
            return dict(map(ascii_encode, pair) for pair in data.items())    
        
        return json.loads(data, object_hook=ascii_encode_dict)
    
    
    
    
class Message(object):
    """ This class is taken out from IBpy 
    everything is the same except it doesn't use __slots__
    and thus allow setting variable values

    """


    def __init__(self, **kwds):
        """ Constructor.

        @param **kwds keywords and values for instance
        """
        
        
        for name in kwds.keys():
            setattr(self, name, kwds[name])
        
        
        #assert not kwds

    def __len__(self):
        """ x.__len__() <==> len(x)

        """
        return len(self.keys())

    def __str__(self):
        """ x.__str__() <==> str(x)

        """
        name = self.typeName
        items = str.join(', ', ['%s=%s' % item for item in self.items()])
        return '<%s%s>' % (name, (' ' + items) if items else '')

    def items(self):
        """ List of message (slot, slot value) pairs, as 2-tuples.

        @return list of 2-tuples, each slot (name, value)
        """
        return zip(self.keys(), self.values())

    def values(self):
        """ List of instance slot values.

        @return list of each slot value
        """
        return [getattr(self, key, None) for key in self.keys()]

    def keys(self):
        """ List of instance slots.

        @return list of each slot.
        """
        return self.__dict__
    
    
    
def dummy():
    topicsEvents = ['accountDownloadEnd', 'execDetailsEnd', 'updateAccountTime', 'deltaNeutralValidation', 'orderStatus',\
          'updateAccountValue', 'historicalData', 'openOrderEnd', 'updatePortfolio', 'managedAccounts', 'contractDetailsEnd',\
          'positionEnd', 'bondContractDetails', 'accountSummary', 'updateNewsBulletin', 'scannerParameters', \
          'tickString', 'accountSummaryEnd', 'scannerDataEnd', 'commissionReport', 'error', 'tickGeneric', 'tickPrice', \
          'nextValidId', 'openOrder', 'realtimeBar', 'contractDetails', 'execDetails', 'tickOptionComputation', \
          'updateMktDepth', 'scannerData', 'currentTime', 'error_0', 'error_1', 'tickSnapshotEnd', 'tickSize', \
          'receiveFA', 'connectionClosed', 'position', 'updateMktDepthL2', 'fundamentalData', 'tickEFP']

    #s = ''.join('\tdef %s(self, items):\n\t\tself.handlemessage("%s", items)\n\n' % (s,s) for s in topicsEvents)
    s = ''.join('# override this function to perform your own processing\n#\tdef %s(self, items):\n#\t\tpass\n\n' % s for s in topicsEvents)
    return s
    

