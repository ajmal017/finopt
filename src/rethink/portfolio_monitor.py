# -*- coding: utf-8 -*-
import sys, traceback
import logging
import json, threading
import copy
from optparse import OptionParser
from time import sleep
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ib.ext.Execution import Execution
from ib.ext.ExecutionFilter import ExecutionFilter
from misc2.helpers import ContractHelper, LoggerNoBaseMessagingFilter, ExecutionFilterHelper
from finopt.instrument import Symbol, Option, InstrumentIdMap, ExecFill
from rethink.option_chain import OptionsChain
from rethink.tick_datastore import TickDataStore
from rethink.portfolio_item import PortfolioItem, PortfolioRules, Portfolio, PortfolioTrades
from rethink.portfolio_column_chart import PortfolioColumnChart,PortfolioColumnChartTM
from rethink.table_model import AbstractTableModel, AbstractPortfolioTableModelListener
from comms.ibc.tws_client_lib import TWS_client_manager, AbstractGatewayListener




class PortfolioMonitor(AbstractGatewayListener, AbstractPortfolioTableModelListener):

    EVENT_PORT_VALUES_UPDATED = 'event_port_values_updated'
    

    def __init__(self, kwargs):
        self.kwargs = copy.copy(kwargs)
        self.twsc = TWS_client_manager(kwargs)
        #AbstractGatewayListener.__init__(self, kwargs['name'])
        #AbstractPortfolioTableModelListener.__init__(self, kwargs['name'])
    
        self.tds = TickDataStore(kwargs['name'])
        self.tds.register_listener(self)
        self.twsc.add_listener_topics(self, kwargs['topics'])
        
        
        
        
        '''
            portfolios: {<account>: <portfolio>}
        '''
        self.portfolios = {}
        self.starting_engine = True
        
        
        '''
            portfolio_charts: {<account>, {'<chart type'>, <chart object ref>...
        '''
        self.portfolio_charts = {}
    

        '''
            store executions: {<account>: <PortfolioTrades>}
        '''
        self.trades = {}
        
    
    def start_engine(self):
        self.twsc.start_manager()
        self.twsc.reqPositions()
 
     
        try:
            def print_menu():
                menu = {}
                menu['1']="Request position" 
                menu['2']="Portfolio dump dtj"
                menu['3']="TDS dump"
                menu['4']="Request account updates"
                menu['5']="Table chart JSON"
                menu['6']="Table index mapping"
                menu['7']="Position Distribution JSON"
                menu['8']="Update TDS table entries by inputting '8 <key> <field> <value>'"
                menu['a']="request exeutions"
                menu['9']="Exit"
        
                choices=menu.keys()
                choices.sort()
                for entry in choices: 
                    print entry, menu[entry]                             
                
            def get_user_input(selection):
                    logging.info('PortfolioMonitor:main_loop ***** accepting console input...')
                    print_menu()
                    while 1:
                        resp = sys.stdin.readline()
                        response[0] = resp.strip('\n')
                        #print response[0]
                                
            
            response = [None]
            user_input_th = threading.Thread(target=get_user_input, args=(response,))
            user_input_th.daemon = True
            user_input_th.start()               
            while True:
                sleep(0.4)
                
                if response[0] is not None:
                    selection = response[0]
                    if selection =='1':
                        self.twsc.reqPositions()
                    elif selection == '2': 
                        for port in self.portfolios.values():
                            
                            port.calculate_port_pl()
                            port.dump_portfolio()
                            self.notify_port_values_updated(port.get_account(), port)
                            #print ''.join('%d:[%6.2f]\n' % (k, v) for k, v in port.calculate_port_pl().iteritems())
                    elif selection == '3': 
                        
                        print self.tds.dump()
                    elif selection == '4': 
                        for acct in self.portfolios.keys():
                            self.twsc.reqAccountUpdates(True, acct)
                    elif selection == '5':
                        for port in self.portfolios.values():
                            print port.get_JSON() 
                    elif selection == '6':
                        for acct in self.portfolios.keys():
                            print self.portfolios[acct].dump_table_index_map() 
                    elif selection == '7':
                        for acct in self.portfolios.keys():
                            pc = PortfolioColumnChart(self.portfolios[acct])
                            print pc.get_JSON()
                            print pc.get_xy_array()
                    elif selection == '8':
                        try:
                            
                            contract_key = response[1]
                            field = int(response[2])
                            if field not in (InstrumentIdMap.idmap.keys()):
                                raise Exception('invalid field')
                            price = float(response[3])
                            logging.info('PortfolioMonitor: manual adjustment to tds table')
                            logging.info('PortfolioMonitor: [%s] field[%s]:%s')
                            self.tds.set_symbol_tick_price(self, contract_key, field, price)
                        except:
                            print "error in input values"
                            continue
                    elif selection == 'a':
                        today = datetime.now()
                        month = int(today.strftime('%m'))
                        past =  today + relativedelta(months=-1)
                        exec_filter = ExecutionFilterHelper.kv2object({'m_time': past.strftime('%Y%m%d %H:%M:%S')}, ExecutionFilter)
                        self.twsc.reqExecutions(exec_filter)
                    elif selection == '9': 
                        self.twsc.gw_message_handler.set_stop()
                        break
                    else: 
                        pass                        
                    response[0] = None
                    print_menu()
                    
        except (KeyboardInterrupt, SystemExit):
            logging.error('PortfolioMonitor: caught user interrupt. Shutting down...')
            self.twsc.gw_message_handler.set_stop() 
            logging.info('PortfolioMonitor: Service shut down complete...')               
        except:
            logging.error('PortfolioMonitor. caught user interrupt. Shutting down...%s' % traceback.format_exc())
 
            self.twsc.gw_message_handler.set_stop() 
            logging.info('PortfolioMonitor: Service shut down complete...')               
        
    def get_kproducer(self):
        # returns a reference to the kafka base producer that we can 
        # use for sending messages
        return self.twsc.gw_message_handler
    
                
    def get_portfolio(self, account):
        try:
            return self.portfolios[account]
        except KeyError:
            # create a new portfolio
            self.portfolios[account] = Portfolio(account)
            #
            # set up portfolio chart objects1
            #
            pcc = PortfolioColumnChartTM('PortfolioColumnChartTM-%s' % account,
                                                                     self.portfolios[account], 
                                                                     self.get_kproducer())
            self.portfolio_charts[account] = {'PortfolioColumnChartTM': pcc}
            self.twsc.add_listener_topics(pcc, [AbstractTableModel.EVENT_TM_REQUEST_TABLE_STRUCTURE, AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED] )            
            logging.info('PortfoioMonitor:get_portfolio creating port and chart object...%s' % account)

        return self.portfolios[account]
    
    def deduce_option_underlying(self, option):
        '''
            given an Option object, return the underlying Symbol object
        '''
        try:
            symbol_id = option.get_contract().m_symbol
            underlying_sectype = PortfolioRules.rule_map['symbol'][symbol_id]
            exchange = option.get_contract().m_exchange
            currency = option.get_contract().m_currency
            expiry = option.get_contract().m_expiry if PortfolioRules.rule_map['expiry'][symbol_id] ==  'same_month' else ''
            contractTuple = (symbol_id, underlying_sectype, exchange, currency, expiry, 0, '')
            logging.info('PortfolioMonitor:deduce_option_underlying. Deduced underlying==> %s' %
                          str(contractTuple))
            return Symbol(ContractHelper.makeContract(contractTuple))
        except KeyError:
            logging.error('PortfolioMonitor:deduce_option_underlying. Unable to deduce the underlying for the given option %s' %
                          ContractHelper.printContract(option.get_contract))
            return None
        
        
    def get_portfolio_option_chain(self, account, underlying):
        
        
        def create_oc_id(account, underlying_id, month):
            return '%s-%s-%s' % (account, underlying_id, month)
        
        underlying_id = underlying.get_contract().m_symbol
        month = underlying.get_contract().m_expiry
        oc_id = create_oc_id(account, underlying_id, month)
        oc = self.portfolios[account].is_oc_in_portfolio(oc_id)
        if oc == None:
            oc = OptionsChain(oc_id)
            oc.register_listener(self)
            oc.set_option_structure(underlying.get_contract(),
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['spd_size'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['multiplier'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['rate'],
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['div'],
                                    month,
                                    PortfolioRules.rule_map['option_structure'][underlying_id]['trade_vol'])
            
            self.portfolios[account].set_option_chain(oc_id, oc) 
            
            
        return oc
    

    def is_interested_contract_type(self, contract_key):
        # given an instrument key, determine whether the contract is relevant
        # to the portfolio monitor
        v = filter(lambda x:x in contract_key, PortfolioRules.rule_map['interested_position_types']['instrument_type'])
        return True if len(v) > 0 else False
    
    def process_position(self, account, contract_key, position, average_cost, extra_info=None):
        
        # obtain a reference to the portfolio, if not exist create a new one 
        port = self.get_portfolio(account)
        port_item =  port.is_contract_in_portfolio(contract_key)
        
            
        if port_item:
            
            
            
            # update the values and recalculate p/l
            port_item.update_position(position, average_cost, extra_info)
            port_item.calculate_pl(contract_key)
            
            #print "process_position %s extra[%s]" % (account, extra_info)
            
            # if the function call is triggered by event accountUpdates from TWS
            # compute the overall portfolio greeks and p/l
            # (that is extra_info is not null)
            if extra_info:
                logging.info('PortfolioMonitor:process_position Recal overall port figures: account[%s]...' % (account))
                port.calculate_port_pl()
                
                self.notify_port_values_updated(account, port)
            
            # dispatch the update to internal listeners
            # and also send out the kafka message to external parties
            self.notify_table_model_changes(account, port, contract_key, mode='U')
            
            logging.info('PortfolioMonitor:process_position. Position updated: %s:[%d]' % (contract_key, port.ckey_to_row(contract_key)))
        # new position 
        else:
            port_item = PortfolioItem(account, contract_key, position, average_cost)
            #port['port_items'][contract_key] = port_item
            port.set_portfolio_port_item(contract_key, port_item)
            instrument = port_item.get_instrument()
            self.tds.add_symbol(instrument)
            self.twsc.reqMktData(instrument.get_contract(), True)
            # option position
            if port_item.get_instrument_type() == 'OPT':
                '''
                    deduce option's underlying
                    resolve associated option chain by month, underlying
                    
                '''
                underlying = self.deduce_option_underlying(instrument)
                if underlying:
                    oc = self.get_portfolio_option_chain(account, underlying)
                    instrument.set_extra_attributes(OptionsChain.CHAIN_IDENTIFIER, oc.get_name())
                    oc.add_option(instrument)
                else:
                    logging.error('PortfolioMonitor:process_position. **** Error in adding the new position %s' % contract_key)

                
            
            self.notify_table_model_changes(account, port, contract_key, mode='I')
            logging.info('PortfolioMonitor:process_position. New position: %s:[%d]' % (contract_key, port.ckey_to_row(contract_key)))
            #port.dump_portfolio()
            
            
              
    
    #         EVENT_OPTION_UPDATED = 'oc_option_updated'
    #         EVENT_UNDERLYING_ADDED = 'oc_underlying_added
    def oc_option_updated(self, event, update_mode, name, instrument):        
        logging.info('oc_option_updated. %s %s' % (event, vars()))
        self.tds.add_symbol(instrument)
        self.twsc.reqMktData(instrument.get_contract(), True)
        
    
    def oc_underlying_added(self, event, update_mode, name, instrument):
        
        logging.info('oc_underlying_added. %s %s' % (event, vars()))
        self.tds.add_symbol(instrument)
        self.twsc.reqMktData(instrument.get_contract(), True)

    
    def tds_event_symbol_added(self, event, update_mode, name, instrument):
        pass
    
        #logging.info('tds_event_new_symbol_added. %s' % ContractHelper.object2kvstring(symbol.get_contract()))
        
    
    def tds_event_tick_updated(self, event, contract_key, field, price, syms):

        
        if field not in [Symbol.ASK, Symbol.BID, Symbol.LAST, Symbol.CLOSE]:
            return
        
        
        
        for s in syms:
            
            # skip position types that are not options or futures 
            # such as currency contracts
            if not self.is_interested_contract_type(contract_key):
                continue

            
            if OptionsChain.CHAIN_IDENTIFIER in s.get_extra_attributes():
                results = {}
                chain_id = s.get_extra_attributes()[OptionsChain.CHAIN_IDENTIFIER]
                #logging.info('PortfolioMonitor:tds_event_tick_updated chain_id %s' % chain_id)
                
                for acct in self.portfolios:
                    
                    #if chain_id  in self.portfolios[acct]['opt_chains'].keys():
                    if chain_id in self.portfolios[acct].get_option_chains():
                        #logging.info('PortfolioMonitor:tds_event_tick_updated --> portfolio opt_chains: [  %s  ] ' % 
                        #             str(self.portfolios[acct]['opt_chains'].keys()))
                        if 'FUT' in contract_key or 'STK' in contract_key:
                            
                            results = self.portfolios[acct].get_option_chain(chain_id).cal_greeks_in_chain(self.kwargs['evaluation_date'], price)
                        else:
                            
                            results[ContractHelper.makeRedisKeyEx(s.get_contract())] =  self.portfolios[acct].get_option_chain(chain_id)\
                                                                                            .cal_option_greeks(s, self.kwargs['evaluation_date'], float('nan'), price)
                    #logging.info('PortfolioMonitor:tds_event_tick_updated. compute greek results %s' % results)
                        
                        #underlying_px = self.portfolios[acct]['opt_chains'][chain_id].get_underlying().get_tick_value(4)
                        
                    def update_portfolio_fields(key_greeks):
                        
                        self.tds.set_symbol_analytics(key_greeks[0], Option.IMPL_VOL, key_greeks[1][Option.IMPL_VOL])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.DELTA, key_greeks[1][Option.DELTA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.GAMMA, key_greeks[1][Option.GAMMA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.THETA, key_greeks[1][Option.THETA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.VEGA, key_greeks[1][Option.VEGA])
                        self.tds.set_symbol_analytics(key_greeks[0], Option.PREMIUM, key_greeks[1][Option.PREMIUM])
                        
                        if self.portfolios[acct].is_contract_in_portfolio(contract_key):
                            self.portfolios[acct].calculate_item_pl(key_greeks[0])
                            self.notify_table_model_changes(acct, self.portfolios[acct], key_greeks[0], mode='U')
                            logging.info('PortfolioMonitor:tds_event_tick_updated. Position updated: %s:[%d]' % (key_greeks[0], self.portfolios[acct].ckey_to_row(key_greeks[0])))
                    
                    if results:
                        #logging.info('PortfolioMonitor:tds_event_tick_updated ....before map')
                        map(update_portfolio_fields, list(results.iteritems()))
                        #logging.info('PortfolioMonitor:tds_event_tick_updated ....after map')
                           
                               
                    
            else:
                for acct in self.portfolios:
                    if self.portfolios[acct].is_contract_in_portfolio(contract_key):
                        self.portfolios[acct].calculate_item_pl(contract_key)
                        self.notify_table_model_changes(acct, self.portfolios[acct], contract_key, mode='U')
                    else:    
                        logging.info('PortfolioMonitor:tds_event_tick_updated ignoring uninterested ticks %s' % contract_key)
                continue
             
        


    def tds_event_symbol_deleted(self, event, update_mode, name, instrument):
        pass


    def tickPrice(self, event, contract_key, field, price, canAutoExecute):
        if not self.is_interested_contract_type(contract_key):
            return

        
        self.tds.set_symbol_tick_price(contract_key, field, price, canAutoExecute)


    def tickSize(self, event, contract_key, field, size):
        #self.tds.set_symbol_tick_size(contract_key, field, size)
        #logging.info('MessageListener:%s. %s: %d %8.2f' % (event, contract_key, field, size))
        pass
        if not self.is_interested_contract_type(contract_key):
            return

            
    def execDetails(self, event, req_id, contract_key, order_id, side, price, avg_price, cum_qty, exec_id, account, exchange, order_ref, exec_time, end_batch):
        
        if not end_batch:
            logging.info("PortfolioMonitor:execDetails: [%s] received %s order_id:[%s] price:[%2f]" % (event, contract_key, order_id, price))
            e = ExecFill()
            
            e.setValues(req_id, contract_key, order_id, side, price, avg_price, cum_qty, exec_id, account, exchange, order_ref, exec_time)
            if account not in self.trades:
                self.trades[account]= PortfolioTrades(account)
            self.trades[account].add_fills(e)
            
        else:
            logging.info("PortfolioMonitor:execDetails: End of execDetails")
            
            for acct in self.portfolios:
                # there may not be any trades returned after midnight
                if acct in self.trades:
                    logging.info("\n".join('execDetails: %s' % t for t in self.trades[acct].get_trades()))
                    self.trades[acct].dump_trades()
                
        

 
    def position(self, event, account, contract_key, position, average_cost, end_batch):


        logging.info('PortfolioMonitor:position account[%s] contract[%s]', account, contract_key)

        if not end_batch:
            #logging.info('PortfolioMonitor:position. received position message contract=%s' % contract_key)
            if self.is_interested_contract_type(contract_key):
                self.process_position(account, contract_key, position, average_cost)
   
        else:
            # to be run once per a/c during start up
            # subscribe to automatic account updates
            if self.starting_engine:
                for acct in self.portfolios.keys():
                    self.twsc.reqAccountUpdates(True, acct)
                    logging.info('PortfolioMonitor:position. subscribing to auto updates for ac: [%s]' % acct)  
                    self.starting_engine = False
                    
    '''
        the 4 account functions below are invoked by AbstractListener.update_portfolio_account.
        the original message from TWS is first wrapped into update_portfolio_account event in 
        class TWS_event_handler and then expanded by AbstractListener.update_portfolio_account
        (check tws_event_hander)
    '''

                
    def updateAccountValue(self, event, key, value, currency, account):  # key, value, currency, accountName):
        self.raw_dump(event, vars())
 
    def updatePortfolio(self, event, contract_key, position, market_price, market_value, average_cost, unrealized_PNL, realized_PNL, account):
        self.raw_dump(event, vars())
        if self.is_interested_contract_type(contract_key):
            
            self.process_position(account, contract_key, position, average_cost, 
                              {'market_price':market_price, 'market_value':market_value, 'unrealized_PNL': unrealized_PNL, 'realized_PNL': realized_PNL})
        
            
    def updateAccountTime(self, event, timestamp):
        self.raw_dump(event, vars())
        logging.info('PortfolioMonitor:updateAccountTime %s' % timestamp)
        for port in self.portfolios.values():
            port.calculate_port_pl()
            self.notify_port_values_updated(port.get_account(), port)
        
        
    def accountDownloadEnd(self, event, account):  # accountName):
        self.raw_dump(event, vars())

 
    def error(self, event, message_value):
        logging.info('PortfolioMonitor:%s. val->[%s]' % (event, message_value))         
        
    def raw_dump(self, event, items):
        del(items['self'])
        logging.info('%s [[ %s ]]' % (event, items))      
        
    
    def notify_table_model_changes(self, account, port, contract_key, mode):
            
        row = port.ckey_to_row(contract_key)
        rvs = port.get_values_at(row)
        #logging.info('---- %s' % str(rvs))
        port.fire_table_row_updated(row, rvs)
        event_type = AbstractTableModel.EVENT_TM_TABLE_ROW_UPDATED if mode == 'U' else AbstractTableModel.EVENT_TM_TABLE_ROW_INSERTED
        self.get_kproducer().send_message(event_type, json.dumps({'source': port.get_object_name(), 'row': row, 'row_values': rvs}))
    
        # notify chart objects to do their thing...
        try:
            pcc = self.portfolio_charts[account]['PortfolioColumnChartTM']
            if self.starting_engine == False:
                if pcc.never_been_run == True:
                    pcc.fire_table_structure_changed(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED, 
                                                  pcc.get_object_name(), None, account, pcc.get_JSON())
                    pcc.never_been_run = False
                    logging.info('PortfolioMonitor:notify_table_model_changes. first time trigger columnchart %d' % pcc.get_last_tally())
                elif pcc.get_last_tally() <> pcc.count_tally():
                    pcc.fire_table_structure_changed(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED, 
                                                  pcc.get_object_name(), None, account, pcc.get_JSON())
                    logging.info('PortfolioMonitor:notify_table_model_changes. tally count %d...dump json' % pcc.get_last_tally())
                    pcc.update_tally_count()
            else:
                pcc.update_tally_count()
                logging.info('PortfolioMonitor:notify_table_model_changes. tally count %d' % pcc.get_last_tally())
#            if mode == 'I':
#                 pcc.fire_table_structure_changed(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED, 
#                                                  pcc.get_object_name(), None, account, pcc.get_JSON())
#            else:
                
#                 row = pcc.ckey_to_row(contract_key)
#                 rvs = pcc.get_values_at(row)
#                 logging.info('PortfolioMonitor:notify_table_model_changes. PortfolioColumnChartTM %d' % row)
                #pcc.fire_table_row_updated(row, rvs)
                  
        except: # KeyError:
            # object does not exist yet?
            # fields have no value causing computing errors? None objects?
            logging.error('**** Error PortfolioMonitor:notify_table_model_changes. %s' % traceback.format_exc() )
    
    # implment AbstractPortfolioTableModelListener
    # handle requests to get data table json
    def event_tm_request_table_structure(self, event, request_id, target_resource, account):
        try:
            if target_resource['class'] == 'Portfolio':
                self.get_kproducer().send_message(AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED,                                               
                                          json.dumps({'source': self.portfolios[account].get_object_name(), 
                                                      'origin_request_id': request_id, 'account': account, 
                                                      'data_table_json': self.portfolios[account].get_JSON()}))
        except:
            logging.error("PortfolioMonitor:event_tm_request_table_structure. Error invoking get_JSON[%s]. Client request id:%s, %s" %
                            (account, request_id, ', '.join(str(e) for e in sys.exc_info())))
            
    def event_tm_table_structure_changed(self, event, source, origin_request_id, account, data_table_json):
        logging.info("[PortfolioColumnChartTM:] received %s  content:[%s]" % (event, data_table_json)    )        
    
    
    
    def notify_port_values_updated(self, account, port):
        try:
            self.get_kproducer().send_message(PortfolioMonitor.EVENT_PORT_VALUES_UPDATED,
                                               json.dumps({'account': account,
                                                     'port_values': self.portfolios[account].get_potfolio_values()}),
                                               )
        except:
            logging.error('**** Error PortfolioMonitor:notify_port_values_updated. %s' % traceback.format_exc() )
        
        
if __name__ == '__main__':
    

    
    kwargs = {
      'name': 'portfolio_monitor',
      'bootstrap_host': 'vorsprung',
      'bootstrap_port': 9092,
      'redis_host': 'localhost',
      'redis_port': 6379,
      'redis_db': 0,
      'tws_host': 'vsu-bison',
      'tws_api_port': 8496,
      'tws_app_id': 38868,
      'group_id': 'PM',
      'session_timeout_ms': 10000,
      'clear_offsets':  False,
      'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/pm.log'},
      'topics': ['position', 'positionEnd', 'tickPrice', 'execDetails', 'update_portfolio_account', 'event_tm_request_table_structure', AbstractTableModel.EVENT_TM_TABLE_STRUCTURE_CHANGED],
      'seek_to_end': ['*'],
      
      
      }

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--clear_offsets", action="store_true", dest="clear_offsets",
                      help="delete all redis offsets used by this program")
    parser.add_option("-g", "--group_id",
                      action="store", dest="group_id", 
                      help="assign group_id to this running instance")
    parser.add_option("-e", "--evaluation_date",
                     action="store", dest="evaluation_date", 
                     help="specify evaluation date for option calculations")   
    
    (options, args) = parser.parse_args()
    if options.evaluation_date == None:
        options.evaluation_date = time.strftime('%Y%m%d') 
    
    for option, value in options.__dict__.iteritems():
        if value <> None:
            kwargs[option] = value
    
    
            
  
      
    logconfig = kwargs['logconfig']
    logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
    logging.basicConfig(**logconfig)        
    logging.getLogger().addFilter(LoggerNoBaseMessagingFilter())
    
    server = PortfolioMonitor(kwargs)
    server.start_engine()
    
          
        