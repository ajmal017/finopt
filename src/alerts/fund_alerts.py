from bs4 import BeautifulSoup
from urllib2 import urlopen
from time import strftime
import time

def send_email(user, pwd, recipient, subject, body):
    import smtplib

    gmail_user = user
    gmail_pwd = pwd
    FROM = user
    TO = recipient if type(recipient) is list else [recipient]
    SUBJECT = subject
    TEXT = body

    # Prepare actual message
    message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print 'successfully sent the mail'
    except:
        print "failed to send mail"


def allianz():
    try:
        url = 'http://www.allianzgi.hk/en/retail/fund-prices?type=&sort=&order=&page_no=&action=&fund_series=3&fund_name=74&fund_class=SHARE_CLASS_RETAIL_CLASS_AM&fund_currency=CCY_DISPLAY_H2_AUD&fund_name_text='
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html5lib')
        lookups = {'unit_price': 'Unit Price','mth_nav': '12 month NAV', 'daily_change_pct': 'daily chg%','valuation_date': 'valuation date'}
        tvs= soup.findAll('td', [k for k in lookups.keys()])
        fund = soup.findAll('td', 'fund_name')[0].a.text
        purchase_px = 10.07
        curr_px = float(soup.find('td', 'unit_price').text)
        percent_chg = (curr_px - purchase_px) / purchase_px * 100
        
        s= '*****************************************\n'
        s+= '      %s\n' % fund
        s+= '\n'
        s+= ', '.join('%s: %s' % (lookups[e.get('class') [0]] if e.get('class')[0] in lookups else 'Daily Chg%', e.text) for e in tvs)
        s+= '\n\n'
        s+= '      purchase price = %0.2f (%0.2f%%)\n' % (purchase_px, percent_chg)
        s+= '\n'
        s+= '\n'
        s+= ' %s\n' % url
        s+= '\n'
        s+= '*****************************************\n'
        return s 
    
        url = 'http://finance.sina.com.cn/fund/quotes/000011/bc.shtml'
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html5lib')
    #     lookups = {'unit_price': 'Unit Price','mth_nav': '12 month NAV', 'daily_change_pct': 'daily chg%','valuation_date': 'valuation date'}
    #     fundname = soup.findAll('div', {'class': 'top_fixed_fund_name'})
    #     funddiv = soup.findAll('div', {'class': 'top_fixed_fund_dwjz'})
    
        blk = soup.findAll('div', {'class': 'fund_info_blk2'})
        fundpx = blk[0].findAll('div', {'class': 'fund_data_item'})[0].find('span', {'class':'fund_data'}).text

    except:
        return 'error extracting allianz fund price'
    return '%s' % (fundpx)

def cn_huaxia():
    try:
        url = 'http://finance.sina.com.cn/fund/quotes/000011/bc.shtml'
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html5lib')
    #     lookups = {'unit_price': 'Unit Price','mth_nav': '12 month NAV', 'daily_change_pct': 'daily chg%','valuation_date': 'valuation date'}
    #     fundname = soup.findAll('div', {'class': 'top_fixed_fund_name'})
    #     funddiv = soup.findAll('div', {'class': 'top_fixed_fund_dwjz'})
    
        blk = soup.findAll('div', {'class': 'fund_info_blk2'})
        fundpx = blk[0].findAll('div', {'class': 'fund_data_item'})[0].find('span', {'class':'fund_data'}).text
    
    
        return 'huaxia %s' % (fundpx)
    except:
        return 'error extracting cn fund price'


def bct_funds():
    
    try:
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
        finfo=  map(fundinfo_extract, anchors)
        return ''.join('%s:%s\n' % (e['name'], e['ask']) for e in finfo)
    except:
        return 'error extracting bct fund price'
    
    
def send_daily_alert():
    user='cigarbar@gmail.com'
    pwd=''
    recipient='@gmail.com'
    
    body = '%s\n%s\n%s' % (allianz(), cn_huaxia(), bct_funds() )
    subject='Daily fund price' 
    send_email(user, pwd, recipient, subject, body)


def retrieve_hk_holidays(year):
    month_names = ['January',
                    'February',
                    'March',
                    'April',
                    'May',
                    'June',
                    'July',
                    'August',
                    'September',
                    'October',
                    'November',
                    'December',
                    ]
    try:
        url = 'http://www.gov.hk/en/about/abouthk/holiday/{{year}}.htm'
        url = url.replace('{{year}}', str(year))
        html = urlopen(url).read()
        soup = BeautifulSoup(html, 'html5lib')
        
        tds = soup.findAll('h3')[0].parent.findAll('td', 'date')
        
        d1 = map(lambda x: (x.text.split(' ')[0], x.text.split(' ')[1]), tds[1:])
        
        return map(lambda x: strftime('%Y%m%d', time.strptime('%s %s %s' % (month_names.index(x[1])+1, x[0], year), "%m %d %Y")), d1)
    except:
        print 'error'

if __name__ == '__main__':      
    #send_daily_alert()
    print retrieve_hk_holidays(2016)
     
#     print allianz()
#     
#     print cn_huaxia()
#     print bct_funds()
    
    
