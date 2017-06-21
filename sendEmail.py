
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText

def send_email(ls):

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('mailrelay.zit.commerzbank.com')

    for alert in ls:
        subscribed_user = alert[0]
        ticker = alert[1]
        benchmark = alert[2]
        if alert[3] == 1:
            calc = "standard deviation"
        else:
            calc = "percentage"
        period = alert[4]

        # Create a text/plain message
        msg = MIMEText("{} has just moved {} {} within last {} days.\n\nThis is an automated email, please do not respond to this address.".format(ticker,benchmark,calc,period))

        msg['Subject'] = 'MarketAlert: Movement in {}'.format(ticker)
        # me == the sender's email address
        msg['From'] = "MarketAlert@commerzbank.com"
        # you == the recipient's email address
        msg['To'] = subscribed_user

        s.send_message(msg)

    s.quit()
