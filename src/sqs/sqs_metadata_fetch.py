import boto3
import json
import yaml

from modules.logger.logger import LoggerInit
from ..utils.utils import getRandomId, getTodaysDate
# from .meta_s3_obj import getS3Object
import os

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name="ap-south-1")
queue_url_global = ''

def sqs_worker(queue_url):
    try:
        global sqs
        global queue_url_global

        queue_url_global = queue_url
        # global queue_url
        
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'chunk-uri'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=5
        )

        if 'Messages' in response:

            received_data = []
            received_recipient_handle = []
            for message in response['Messages']:

                received_data.append(message['Body'])
                ReceiptHandle = message['ReceiptHandle']
                received_recipient_handle.append(ReceiptHandle)

                deleteFromSqs(ReceiptHandle)

            # DBUG LOG
            # logger.info(f"received data: {received_data}")

            return {
                "Status": "SUCCESS",
                "RecivedList": received_data,
                "RecipientHandleList": received_recipient_handle,
                "ReceiptHandle": ReceiptHandle
            }

        else:
            return {
                "Status": "SQS_EMPTY",
                "ErrorMessage": "SQS empty"
            }


    except Exception as e:
        logger.info(f"Exception occured {e}")
        return {
            "Status": "ERROR",
            "ErrorMessage": e
        }
  
def deleteFromSqs(ReceiptHandle):
    logger.info(f"SQS: Deleting from SQS ReceiptHandle: {ReceiptHandle}")
    delete_message = sqs.delete_message(
        QueueUrl=queue_url_global,
        ReceiptHandle=ReceiptHandle
    )

def addToSQS(data, queue_url):
    date, month, year = getTodaysDate()
    batchID = f'{date}00{month}00{year}'
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={},
        MessageBody=data,
        MessageGroupId="inno_qr_out_" + str(batchID),
        MessageDeduplicationId="inno_qr_out_"+str(getRandomId(6))
    )

    return response

def addToSQSFifo(data, queue_url):
    date, month, year = getTodaysDate()
    batchID = f'{date}00{month}00{year}'
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={},
        MessageBody=data,
        MessageGroupId="inno_qr_out_dlq_" + str(batchID),
        MessageDeduplicationId="inno_qr_out_dlq"+str(getRandomId(6))
    )

    return response
