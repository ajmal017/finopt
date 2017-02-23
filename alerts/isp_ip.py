from bs4 import BeautifulSoup
from urllib2 import urlopen
from time import strftime
import time, sys

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


def send_ip_alert(curr_ip, old_ip):
    user='cigarbar@gmail.com'
    pwd='taipeii0i'
    recipient='larry1chan@gmail.com'
    
    body = 'different ip detected. Update new record at no-ip to avoid hosts not reachable\nNew: [%s]  Old:[%s]\n' % (curr_ip, old_ip)
    subject='IP changed %s' % curr_ip 
    send_email(user, pwd, recipient, subject, body)


def isp_assigned_ip(iphist):
    curr_ip = None
    try:
        url = 'http://ipecho.net/plain'
        curr_ip = urlopen(url).read()
        
        
    except:
        return 'error getting the current ip address!'

    try:
        f = open(iphist)
        old_ip = f.readline()
        if old_ip <> curr_ip:
            send_ip_alert(curr_ip,  old_ip)
            print 'different ip detected. Update new record at no-ip to avoid hosts not reachable'
            f.close()
            f = open(iphist, 'w')
            f.write(curr_ip)
            f.close()
        else:
            print 'same ip - nothing to do'
    except IOError:
        old_ip = "no previous ip record!"
        f = open(iphist, 'w')
        f.write(curr_ip)
        f.close()
        send_ip_alert(curr_ip,  old_ip)
        
    


if __name__ == '__main__':      
    #send_daily_alert()
    if len(sys.argv) != 2:
        print("Usage: %s <path to iphistory.dat>" % sys.argv[0])
        exit(-1)   
        
    isp_assigned_ip(sys.argv[1])
     
#     print allianz()
#     
#     print cn_huaxia()
#     print bct_funds()
    
    