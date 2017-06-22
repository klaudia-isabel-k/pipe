
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText

def main(subscribed_user,security,benchmark,calc_id,period,today_calc,today_benchmark):

    if str(subscribed_user) == "nan":
        print("Subscribed user does not exist in the database.")
    else:
    
        # Send the message via our own SMTP server.
        s = smtplib.SMTP('mailrelay.zit.commerzbank.com')

        # Choose calculation label depending on the id
        if calc_id == 1:
            calc = "percentage"
        if calc_id == 2:
            calc = "standard deviation"

        # Construct alert message
        msg_text = "{} has just moved to {} which is over the benchmark at {} calculated for {} {} movement over last {} days.".format(security,round(today_calc,5),round(today_benchmark,2),round(benchmark,2),calc,period)

        # Add automated footnote message
        msg_text = msg_text + "\n\nThis is an automated email, please do not respond to this address."

        # Create a text/plain message
        msg = MIMEText(msg_text)

        # Construct Subject Email
        msg['Subject'] = 'MarketAlert: Movement in {} ({})'.format(security,calc)

        # Choose Sender's Email
        msg['From'] = "MarketAlert@commerzbank.com"

        # Choose Receiver's Email
        msg['To'] = subscribed_user

        # Send message
        s.send_message(msg)

        # Close SMTP server
        s.quit()
