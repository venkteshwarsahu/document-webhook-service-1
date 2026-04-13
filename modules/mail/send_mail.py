from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import os
import boto3
from botocore.config import Config
from modules.logger.logger import LoggerInit

my_config = Config(
    region_name = 'ap-south-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        # 'mode': 'standard'
    }
)

client = boto3.client('ses', config=my_config)
# msg = MIMEMultipart()

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)
loggercolor = LoggerInit('_'+module_name, color=True)

def send_mail(message,
              subject,
              source_email,
              to_mailers):
    response = client.send_email(
        Source=source_email,
        Destination={
            'ToAddresses': to_mailers
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': message,
                    'Charset': 'UTF-8'
                }
            }
        }
    )

    return response

def sendMailWithAttachment(message,
                           subject,
                           attachment,
                           source_email,
                           to_mailers,
                           filename='status.csv'):

    for sender_mail in to_mailers:
        msg = MIMEMultipart()
        logger.info(f'Sending status mail to {sender_mail}')
        msg['Subject'] = subject
        # msg['From'] = source_email
        msg['to'] = sender_mail

        # what a recipient sees if they don't use an email reader
        msg.preamble = 'Status mail.\n'

        # the message body
        part = MIMEText(message)
        msg.attach(part)

        # the attachment
        try:
            part = MIMEApplication(bytes(attachment))
        except Exception as e:
            logger.error(f'attachmentException: {e}')
            logger.info(f'retrying registering attachment..')
            part = MIMEApplication(attachment)

        logger.debug(f'MIME application attached starting')    
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

        logger.debug(f'message attached')
        # and send the message
        logger.debug(f'Data: {msg.as_string()}')
        try:
            result = client.send_raw_email(RawMessage={
                    'Data':msg.as_string(),
                }
                , Source=source_email
                , Destinations=[sender_mail])
            
            del msg
        except Exception as e:
            logger.error(f'Error in sending mail to {sender_mail}: {e}')
            result = None
            del msg
    return result
    

####################@@@ TEST BLOCK @@@#########################
"""
from ..database.dynamodb_export import exportFromDynamoDB

table_name = 'Aadhar_Mask_OCR'
batch_meta = {}
batch_meta["name"] = '08012021_1'
csv_attachment = exportFromDynamoDB(table_name, 
                                    f'batch={batch_meta["name"]}')

message = 'test main body'
subject = 'test subject'
source_address = 'anurag.vishwakarma@innodeed.com'
subscribers_list = ['anurag.vishwakarma@innodeed.com', 'abhishek.vishwakarma@innodeed.com', 'ravi.sahu@innodeed.com', 'praveen.kanda@innodeed.com']
stdprint(f'CSV attachment: {csv_attachment.read()}, type: {type(csv_attachment.read())}')

csv_attachment.seek(0)
csv_string = csv_attachment.read()
raw_send_mail_resp = sendMailWithAttachment(message,
                                            subject,
                                            csv_string,
                                            source_email=source_address,
                                            to_mailers=subscribers_list,
                                            filename=f'status_{batch_meta["name"]}.csv')
"""
