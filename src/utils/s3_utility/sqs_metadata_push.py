import boto3
import json
import yaml
from ..utils import stdprint, SQS, Config
from ..assets.logger import LoggerInit
from .meta_s3_obj import getS3Object
from random import randint
from datetime import datetime
import os

module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=True)
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name="us-west-2")
queue_url = ''

class Utils:
    @staticmethod
    def getRandomId(n):
        range_start = 10**(n-1)
        range_end = (10**n)-1
        uidd = randint(range_start, range_end)

        dt_obj = datetime.now()
        uid = dt_obj.timestamp() * 1000
        return int(uid)+int(uidd)

class SQS_Push(SQS, Config, Utils):
    """
        This class is responsible to push data to sqs
        Inharits the sqs class from config
    """

    def __init__(self):
        self.s3 = boto3.resource('s3')
        sqs_obj = SQS(Config.config_path)
        self.sqs = sqs_obj.getSQSObject()
        self.cpu_queue_url = sqs_obj.getCPUQueueURL()
    
    def push_to_sqs(self, data, batch):
        """Push data to sqs"""
        logger.info(f'self.cpu_queue_url')
        response = self.sqs.send_message(
                        QueueUrl=self.cpu_queue_url,
                        MessageAttributes={},
                        MessageBody=(json.dumps(data)),
                        MessageGroupId="sqs_inno" + str(batch),
                        MessageDeduplicationId="sqs_inno"+str(Utils.getRandomId(6))
                    )

        # logger.info(f'sqs response: {response}')
        return response