# -*- coding: utf-8 -*-
import sys, traceback
import logging
from misc2.helpers import ConfigMap
from optparse import OptionParser
import time, datetime
import os, wget
import json, ntpath


class HkexStatDownloader():
    
    def __init__(self, kwargs):
        
        
        #print '\n'.join("x:{##, '%s'}" % (k) for k,v in sorted(kwargs.iteritems()))
        self.kwargs = kwargs
        self.data_list = eval(kwargs['data_list'])
        if 'auto' in kwargs.keys():
            self.download_auto()
        else:
            self.download_stat(kwargs['download_types'], kwargs['day'], kwargs['output_path'], 'compress')
    

    # day parameter must be in YYMMDD format
    def is_valid_day(self, day):
        
        the_day = datetime.datetime.strptime(day,'%y%m%d')
        print the_day
        holidays = eval(self.kwargs['hk_holiday'])
        # format as YYYY
        year = the_day.strftime('%Y')
        weekday = the_day.weekday()
        if the_day.strftime('%m%d') not in holidays[year] and weekday not in [5,6]:
            return True
        else:  
            return False
            
    
    def download_auto(self):
        download_type = 'abcdefghijklm'
        
        
        today = datetime.datetime.now().strftime('%y%m%d')
        if self.is_valid_day(today):
            self.download_stat(download_type, today , kwargs['output_path'])
        else:
            logging.info('%s not a business day, no download' % today)  
        
    # day_str in yymmdd
    def download_stat(self, download_type, day_str, output_path, compress=False):
        
        if not self.is_valid_day(day_str):
            logging.info('%s not a business day, no download' % day_str)
            return
            
        dir = '%s%s' % (output_path, day_str)
        if not os.path.exists(dir):
            os.makedirs(dir)
            
        for ch in download_type:
            link = self.kwargs[self.data_list[ch][1]]
            
            
            # special check for 'k' type dayily short sell
            if day_str <> datetime.datetime.now().strftime('%y%m%d') and ch == 'k':
                continue
            link = link % day_str if ch <> 'k' else link
            try:
                path = '%s/%s' % (dir, ntpath.basename(link))
                logging.info('HkexStatDownloader:[%c] url:%s path %s to download' % (ch, link, path))
                
                wget.download(link, path)
            except:
                print 'exception: check log for additional error messages.'
                logging.error(traceback.format_exc())
                
    




    
    
    

if __name__ == '__main__':
    

    
#     kwargs = {
#       'logconfig': {'level': logging.INFO, 'filemode': 'w', 'filename': '/tmp/daily_download.log'},
#       
#       }

    usage = """usage: %prog [options]
    
            a:{'HSI options after market HTML', 'dha_url'}
            b:{'HSI options after market ZIP', 'dza_url'},
            c:{'HSI options normal hours ZIP', 'dzn_url'},
            d:{'HSI futures normal hours HTML', 'fhn_url'},
            e:{'HSI futures normal hours ZIP', 'fzn_url'},
            f:{'HHI CN futures normal hours HTML', 'hhn_url'},
            g:{'HHI CN futures normal hours ZIP', 'hzn_url'},
            h:{'HHI CN options normal hours HTML', 'ohn_url'},
            i:{'HHI CN options normal hours ZIP', 'ozn_url'},
            j:{'Cash market daily quotes HTML ', 'shd_url'},
            k:{'Cash market short sell HTML', 'shs_url'},
            l:{'HSI volatility HTML', 'vh_url'},
            m:{'HSI volatility ZIP', 'vz_url'}
            """
    
    
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--download_types",
                      action="store", dest='download_types')
    parser.add_option("-a", "--auto",
                      action="store_true", dest='auto')
    
    parser.add_option("-s", "--day",
                      action="store", dest='day')    
    parser.add_option("-f", "--config_file",
                      action="store", dest="config_file", 
                      help="path to the config file")
    
    
    (options, args) = parser.parse_args()
    try:
        print options
        kwargs = ConfigMap().kwargs_from_file(options.config_file)
        for option, value in options.__dict__.iteritems():
             if value <> None:
                 kwargs[option] = value
        logconfig = kwargs['logconfig']
        logconfig['format'] = '%(asctime)s %(levelname)-8s %(message)s'    
        logging.basicConfig(**logconfig)        
        logging.info('config settings: %s' % kwargs)
        
        
        hkex = HkexStatDownloader(kwargs)
            
            
    
    except:
        
        print 'exception: check log for additional error messages.'
        logging.error(traceback.format_exc())
    


