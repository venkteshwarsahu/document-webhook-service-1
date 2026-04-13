import os
from modules.logger.logger import LoggerInit
from modules.mail.send_mail import sendMailWithAttachment

#initiate logger
module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=False)


def send_mail(attachment, timestamp, config):

    mail = config['MAIL']
    
    date, month, year = timestamp[0], timestamp[1], timestamp[2]

    mail_subject = mail['SUBJECT'].replace('$', f'{date}-{month}-{year}')
    mail_body = mail['BODY'].replace('$', f'{date}-{month}-{year}')
    mailers_list = mail['MAIL_IDS']
    source = mail['SOURCE_EMAIL']
    mail_attachment_filename = f'MIS_{date}-{month}-{year}.csv'


    sendMailStatus = sendMailWithAttachment(message = mail_body,
                                                subject = mail_subject,
                                                attachment = attachment,
                                                source_email = source,
                                                to_mailers = mailers_list,
                                                filename=mail_attachment_filename)
    
    if sendMailStatus is None:
        logger.error('Error in sending mail')
        logger.info(f'Send mail status: {sendMailStatus}')
        return False

    return True

