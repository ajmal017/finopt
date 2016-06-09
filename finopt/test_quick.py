# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from urllib2 import urlopen
from optcal import cal_option
import ystockquote 


def bct_info():
    
    url = 'http://www.bcthk.com/BCT/html/eng/page/WMP0240/FIF0100/fund.jsp'
    html = urlopen(url).read()
    soup = BeautifulSoup(html, 'html5lib')
    lookups = ['BCT (Pro) Absolute Return Fund', 'BCT (Pro) Global Bond Fund', 'BCT (Pro) Hong Kong Dollar Bond Fund', 'BCT (Pro) MPF Conservative Fund']
#     for e in soup.findAll('a', 'green03'):
#         print e.text 
        
    anchors= filter(lambda x: x.text[:(x.text.find('Fund')+4)] in lookups, soup.findAll('a', 'green03'))
    
    def fundinfo_extract(felem):
        node = felem.parent.parent.findAll('td')
        e = {}
        e['name'] = felem.text[:(felem.text.find('Fund')+4)]
        e['bid'] = node[2].text
        e['ask'] = node[3].text
        return e
    print map(fundinfo_extract, anchors)
    
#def cal_option(spot, strike, callput, evaldate, exdate, rate, div, vol):
# 
# 
def run_tests():
    strikes = range(16000, 24000, 200)
    terms = {'spot': 20300, }  
    series = [('20160330', terms) , ('20160428', terms), ('20160630', terms)]
    
    
        
    results = {}
    
    def compute(ts):
        terms = ts[1]
#         map(calc, )
        results = cal_option(terms['spot'], terms['strike'], terms['callput'],  
                             terms['evaldate'], terms['exdate'], terms['rate'], terms['div'], terms['vol'])
        return results
    
    map(compute, series)

def get_hist_price():
    #data= ystockquote.get_historical_prices('^FTSE', '20151201', '20160329')
    data = [['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Clos'], ['2016-03-24', '6199.100098', '6199.100098', '6090.00', '6106.50', '606523700', '6106.5'], ['2016-03-23', '6192.700195', '6216.799805', '6171.100098', '6199.100098', '603287100', '6199.10009'], ['2016-03-22', '6184.600098', '6193.50', '6110.399902', '6192.700195', '716814900', '6192.70019'], ['2016-03-21', '6189.600098', '6215.299805', '6154.100098', '6184.600098', '516111500', '6184.60009'], ['2016-03-18', '6201.100098', '6237.00', '6186.200195', '6189.600098', '1095973700', '6189.60009'], ['2016-03-17', '6175.50', '6220.00', '6125.700195', '6201.100098', '807636500', '6201.10009'], ['2016-03-16', '6140.00', '6186.200195', '6134.299805', '6175.50', '679738500', '6175.5'], ['2016-03-15', '6174.600098', '6174.600098', '6114.799805', '6140.00', '679303900', '6140.0'], ['2016-03-14', '6139.799805', '6197.799805', '6139.799805', '6174.600098', '668605400', '6174.60009'], ['2016-03-11', '6036.700195', '6150.899902', '6036.700195', '6139.799805', '700052600', '6139.79980'], ['2016-03-10', '6146.299805', '6203.399902', '6036.700195', '6036.700195', '1024618000', '6036.70019'], ['2016-03-09', '6125.399902', '6174.799805', '6118.200195', '6146.299805', '735581400', '6146.29980'], ['2016-03-08', '6182.399902', '6182.50', '6101.899902', '6125.399902', '886111500', '6125.39990'], ['2016-03-07', '6199.399902', '6216.100098', '6125.600098', '6182.399902', '793417200', '6182.39990'], ['2016-03-04', '6130.50', '6204.100098', '6130.50', '6199.399902', '840450500', '6199.39990'], ['2016-03-03', '6147.100098', '6173.700195', '6108.399902', '6130.50', '876988500', '6130.5'], ['2016-03-02', '6152.899902', '6194.00', '6097.799805', '6147.100098', '878482700', '6147.10009'], ['2016-03-01', '6097.100098', '6153.799805', '6070.50', '6152.899902', '905617400', '6152.89990'], ['2016-02-29', '6096.00', '6105.00', '6033.200195', '6097.100098', '931711000', '6097.10009'], ['2016-02-26', '6012.799805', '6115.399902', '6012.799805', '6096.00', '833169500', '6096.0'], ['2016-02-25', '5867.200195', '6029.00', '5867.200195', '6012.799805', '000', '6012.79980'], ['2016-02-24', '5962.299805', '5966.700195', '5845.600098', '5867.200195', '809642000', '5867.20019'], ['2016-02-23', '6037.700195', '6037.700195', '5954.200195', '5962.299805', '725213500', '5962.29980'], ['2016-02-22', '5950.200195', '6065.799805', '5950.200195', '6037.700195', '797443800', '6037.70019'], ['2016-02-19', '5972.00', '6001.200195', '5916.299805', '5950.200195', '672114300', '5950.20019'], ['2016-02-18', '6030.299805', '6036.50', '5948.299805', '5972.00', '785745600', '5972.0'], ['2016-02-17', '5862.200195', '6030.299805', '5862.200195', '6030.299805', '921163400', '6030.29980'], ['2016-02-16', '5824.299805', '5880.700195', '5812.50', '5862.200195', '747963000', '5862.20019'], ['2016-02-15', '5707.600098', '5844.50', '5707.600098', '5824.299805', '702916100', '5824.29980'], ['2016-02-12', '5537.00', '5707.600098', '5537.00', '5707.600098', '1051089200', '5707.60009'], ['2016-02-11', '5672.299805', '5672.299805', '5499.50', '5537.00', '000', '5537.0'], ['2016-02-10', '5632.200195', '5712.799805', '5616.899902', '5672.299805', '964970500', '5672.29980'], ['2016-02-09', '5689.399902', '5739.299805', '5596.299805', '5632.200195', '1112497500', '5632.20019'], ['2016-02-08', '5848.100098', '5882.399902', '5666.100098', '5689.399902', '968728200', '5689.39990'], ['2016-02-05', '5898.799805', '5945.899902', '5839.399902', '5848.100098', '954158300', '5848.10009'], ['2016-02-04', '5837.100098', '5938.100098', '5831.100098', '5898.799805', '1021666600', '5898.79980'], ['2016-02-03', '5922.00', '5924.600098', '5791.00', '5837.100098', '996202200', '5837.10009'], ['2016-02-02', '6060.100098', '6060.50', '5889.600098', '5922.00', '986962800', '5922.0'], ['2016-02-01', '6083.799805', '6115.100098', '5993.799805', '6060.100098', '808643200', '6060.10009'], ['2016-01-29', '5931.799805', '6083.799805', '5931.799805', '6083.799805', '988153400', '6083.79980'], ['2016-01-28', '5990.399902', '6020.50', '5889.399902', '5931.799805', '892436800', '5931.79980'], ['2016-01-27', '5911.50', '5990.399902', '5870.799805', '5990.399902', '875026000', '5990.39990'], ['2016-01-26', '5877.00', '5919.200195', '5771.399902', '5911.50', '852296000', '5911.5'], ['2016-01-25', '5900.00', '5933.50', '5851.799805', '5877.00', '803777200', '5877.0'], ['2016-01-22', '5773.799805', '5926.899902', '5773.799805', '5900.00', '879968100', '5900.0'], ['2016-01-21', '5673.600098', '5781.200195', '5659.200195', '5773.799805', '1089335700', '5773.79980'], ['2016-01-20', '5876.799805', '5876.799805', '5639.899902', '5673.600098', '1035052400', '5673.60009'], ['2016-01-19', '5779.899902', '5915.700195', '5779.899902', '5876.799805', '808697900', '5876.79980'], ['2016-01-18', '5804.100098', '5852.100098', '5766.50', '5779.899902', '718879400', '5779.89990'], ['2016-01-15', '5918.200195', '5934.600098', '5769.200195', '5804.100098', '1084338100', '5804.10009'], ['2016-01-14', '5961.00', '5961.00', '5829.299805', '5918.200195', '1067078800', '5918.20019'], ['2016-01-13', '5929.200195', '6011.100098', '5929.200195', '5961.00', '809502200', '5961.0'], ['2016-01-12', '5871.799805', '5985.799805', '5866.700195', '5929.200195', '806737100', '5929.20019'], ['2016-01-11', '5912.399902', '5941.899902', '5871.799805', '5871.799805', '817313600', '5871.79980'], ['2016-01-08', '5954.100098', '6013.399902', '5912.399902', '5912.399902', '809964200', '5912.39990'], ['2016-01-07', '6073.399902', '6073.399902', '5888.00', '5954.100098', '1054315500', '5954.10009'], ['2016-01-06', '6137.200195', '6137.200195', '6018.700195', '6073.399902', '699957700', '6073.39990'], ['2016-01-05', '6093.399902', '6166.299805', '6079.200195', '6137.200195', '624070800', '6137.20019'], ['2016-01-04', '6242.299805', '6242.299805', '6071.00', '6093.399902', '686232700', '6093.39990'], ['2015-12-31', '6274.100098', '6278.299805', '6233.00', '6242.299805', '161427100', '6242.29980'], ['2015-12-30', '6314.600098', '6314.600098', '6261.50', '6274.100098', '327817500', '6274.10009'], ['2015-12-29', '6254.600098', '6314.600098', '6245.200195', '6314.600098', '448441700', '6314.60009'], ['2015-12-24', '6241.00', '6259.899902', '6236.899902', '6254.600098', '108851800', '6254.60009'], ['2015-12-23', '6083.100098', '6248.50', '6083.100098', '6241.00', '578630000', '6241.0'], ['2015-12-22', '6034.799805', '6090.600098', '6031.799805', '6083.100098', '424323400', '6083.10009'], ['2015-12-21', '6052.399902', '6114.00', '6034.799805', '6034.799805', '545001400', '6034.79980'], ['2015-12-18', '6102.50', '6105.600098', '6051.700195', '6052.399902', '1234966800', '6052.39990'], ['2015-12-17', '6061.200195', '6160.799805', '6061.200195', '6102.50', '725125100', '6102.5'], ['2015-12-16', '6017.799805', '6089.299805', '6016.299805', '6061.200195', '750664600', '6061.20019'], ['2015-12-15', '5874.100098', '6036.700195', '5874.100098', '6017.799805', '811231700', '6017.79980'], ['2015-12-14', '5952.799805', '6009.899902', '5871.899902', '5874.100098', '781019600', '5874.10009'], ['2015-12-11', '6088.100098', '6088.100098', '5949.799805', '5952.799805', '805984900', '5952.79980'], ['2015-12-10', '6126.700195', '6127.100098', '6080.00', '6088.100098', '838093600', '6088.10009'], ['2015-12-09', '6135.200195', '6175.799805', '6101.200195', '6126.700195', '751852900', '6126.70019'], ['2015-12-08', '6223.50', '6224.899902', '6120.700195', '6135.200195', '790574400', '6135.20019'], ['2015-12-07', '6238.299805', '6287.200195', '6215.200195', '6223.50', '542255600', '6223.5'], ['2015-12-04', '6275.00', '6277.600098', '6219.50', '6238.299805', '662790200', '6238.29980'], ['2015-12-03', '6420.899902', '6444.700195', '6275.00', '6275.00', '793356800', '6275.0'], ['2015-12-02', '6395.700195', '6447.299805', '6395.200195', '6420.899902', '566098900', '6420.89990'], ['2015-12-01', '6356.100098', '6402.399902', '6356.100098', '6395.700195', '784743400', '6395.70019']]
    data = data[1:]
    print len(data)
    d1d2 = map(lambda i: (float(data[i*2][6]), float(data[i*2+1][6])), range(len(data)/2))
    print len(d1d2)
    dchng = map(lambda x: (1 if (x[1]-x[0])/x[0]> 0 else 0, x[0], x[1]), d1d2)
    print dchng

if __name__ == '__main__':      

    #run_tests()
    #bct_info()
    get_hist_price()
    
