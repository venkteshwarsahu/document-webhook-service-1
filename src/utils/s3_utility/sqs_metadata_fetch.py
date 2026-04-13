import boto3
import json
import yaml
from ..utils import stdprint, SQS
from modules.logger.logger import LoggerInit
from .meta_s3_obj import getS3Object
import os

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name="us-west-2")
queue_url = ''

def sqs_worker(queue_url):
    try:
        global sqs
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

            return {
                "Status": "SUCCESS",
                "RecivedList": received_data,
                "RecipientHandleList": received_recipient_handle
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
        QueueUrl=queue_url,
        ReceiptHandle=ReceiptHandle
    )

def getSQSMetadata(config_file):
    resp = {}
    global sqs
    global queue_url

    logger.info("SQS: Fetching from sqs..")
    sqs_obj = SQS(config_file)
    logger.info("Fetching main queue url from config..")
    sqs = sqs_obj.getSQSObject()
    logger.info("Fetching priority queue url from config..")
    
    logger.info("SQS: Fetching from sqs..")
    try:
        # fetch from priority queue (if exists)
        queue_url = sqs_obj.getPriorityQueueURL()    
        fetch_from_sqs = sqs_worker(queue_url)

        # priority queue is empty, set queue variable to main queue url
        if fetch_from_sqs['Status'] == "SQS_EMPTY":
            queue_url = sqs_obj.getSQSUrl()
            fetch_from_sqs = sqs_worker(queue_url)

        logger.info(f'SQS: metadata fetch status: {fetch_from_sqs["Status"]}')
        if fetch_from_sqs['Status'] == 'SUCCESS':
            # RecivedObject = fetch_from_sqs['RecivedList']
            
            RecivedObject = fetch_from_sqs['RecivedList'][0].replace("'", '"')
            RecivedObject = json.loads(RecivedObject)
            # logger.info(f'{RecivedObject} .... ')

            try:
                # logger.info(f'recieved object is: {RecivedObject}')
                RecivedObject["info"]["queue_url"] = queue_url
                s3_path_location = RecivedObject["info"]["location"]
                obj_response = getS3Object(RecivedObject, s3_path_location, s3)

                # get message from getS3Object method resopnse object
                message = obj_response['message']

                #DEBUG LOG
                logger.info(f'SQS: message s3 object: {message}')   
                resp["Message"] = message
                resp["Info"] = RecivedObject["info"]
                resp["Status"] = "SUCCESS"
                resp["innoBatchID"]= RecivedObject["info"]["innoBatchID"]

                # append JSONBucketName and ItemName in resp
                resp['JSONBucketName'] = obj_response['JSONBucketName']
                resp['JSONItemName'] = obj_response['JSONItemName']
                resp['BatchJSONAbsPath'] = obj_response['BatchJSONAbsPath']
                resp['FullSQSResponse'] = RecivedObject
                
                return resp

            except Exception as e:
                logger.error(f"SQS: Exception in {module_name}: {e}")
                deleteFromSqs(fetch_from_sqs['RecipientHandleList'][0])
                getSQSMetadata(config_file)
        else:
            logger.error("Error")
            return fetch_from_sqs

    except Exception as e:
        logger.error(f"SQS: Exception in {module_name}: {e}")
