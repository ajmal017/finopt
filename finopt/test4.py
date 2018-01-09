import ystockquote
import redis
import json

from ystockquote import Alphavantage

def download_historical_px(code):
    rs = redis.Redis('localhost', 6379,0)
    av = Alphavantage()
    #d = av.time_series_daily_adjusted('AUDUSD=X', 'csv', 'full')
    
    d = av.time_series_daily_adjusted(code, 'csv', 'full')
    rs.set('ts-%s' % code, json.dumps(d))
    


def load_historical(code):
    rs = redis.Redis('localhost', 6379,0)
    return json.loads(rs.get('ts-%s' % code)) 


def get_daily_percent_change(ts):
    #return ', '.join('[new Date("%s"), %s]' % (elem[0], float(elem[4]))  for elem in ts)
    ts = filter(lambda x: float(x[4]) <> 0.0, ts)
    print ts
    return ', '.join('[new Date("%s"), %0.6f]' % (elem[0], (float(elem[4]) - float(elem[1])) / float(elem[4]))  for elem in ts)
    
def get_daily_close(ts):
    
    return ', '.join('[new Date("%s"), %s]' % (elem[0], float(elem[4]))  for elem in ts)
    


if __name__ == '__main__':
    #download_historical_px('000001.SS')
#     ts = load_historical('000001.SS')
#     ts = filter(lambda x:int(x[0][0:4]) >= 2017, ts[1:])
#     print get_daily_percent_change(ts)

    #download_historical_px('^HSI')
    ts = load_historical('^HSI')
    ts = filter(lambda x:int(x[0][0:4]) >= 2017, ts[1:])
    print get_daily_close(ts)
    print get_daily_percent_change(ts)
    