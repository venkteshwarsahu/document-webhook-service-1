# import boto3
import os
import sys
import json
from ..utils import stdprint
from modules.logger.logger import LoggerInit

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)

"""
example sqs recieved data:
resp = {
...     "info": {
...     'name': 'batch1',
...     'location': 'inno-dataholder/mask_2020-08-10T14:12:12/tiff/',
...     'fileCount': 100,
...     'uploadStartDate': '2020-08-10T14:12:12',
...     'uploadEndDate': '2020-08-10T14:12:12'
...   },
...   "files": {
...     "1725101003254.tif": {
...       "type": "tif"
...     },
...     "1725801003182": {
...       "type": "tif"
...     }
...   }
... }

example response:

"""


def getS3Object(recieved_sqs_object, location, s3):
    # recieved_sqs_object = json.loads(recieved_sqs)
    # logger.info(f'recieved/ {recieved_sqs_object}')
    BucketName = recieved_sqs_object['batch']['bucket_name']
    ItemName = recieved_sqs_object['batch']['dest_bucket_prefix_url']

    # INFO LOG
    logger.info(f'S3 bucket name: {BucketName}')
    logger.info(f'Metadata JSON ItemName: {ItemName}')

    # getting filename and filetype
    obj = s3.Object(BucketName, ItemName)
    body = obj.get()['Body'].read()

    # # load json body
    # json_body = json.loads(body.decode("utf-8"))
    # # get image lists
    # resp = json_body["files"]

    resp = json.loads(body.decode("utf-8"))

    stdprint(resp)

    try:
        resp = resp["files"]
    except:
        resp = json.loads(body.decode("utf-8"))


    message = []
    for filename, obj in resp.items():
        obj["image_S3_path"] = location
        
        # get filetype from object
        filetype = obj['type']

        # check if filename already contains extension
        if filetype in filename:
            pass
        else:
            filename = str(filename)+'.'+str(filetype)
        
        obj["name"] = filename

        stdprint(f'registering object..')
        stdprint(f'{obj}')
        stdprint()
        
        message.append(obj)

    # DEBUG LOG
    # logger.info(f'message: {message}')
    method_response = {
        'message': message,
        'JSONBucketName': BucketName,
        'JSONItemName': ItemName,
        'BatchJSONAbsPath': os.path.join(BucketName, ItemName)
    }

    # return message
    return method_response
