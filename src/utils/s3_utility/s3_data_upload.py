# USAGE
# python PushTpS3Observer.py <folder_path> <folder_path> <folder_path> ...
# python PushToS3Observer.py Datasets/train Datasets/test Datasets/val

import sys
import time
import logging

# from watchdog.observers.polling import PollingObserver as Observer
# from watchdog.events import PatternMatchingEventHandler
from pathlib import Path
import boto3
import shutil
import yaml
import json
from botocore.exceptions import ClientError
from modules.logger.logger import LoggerInit
import threading
import os
import base64

import random

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)
loggercolor = LoggerInit("_" + module_name, color=True)

table_name = ""
json_filename_registry = {}

s3_client = boto3.client("s3")
s3_resource = boto3.resource('s3')

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


class ProcessMessage:
    @staticmethod
    def replaceSymbol(orig_message, src_batch_name, symbol="#"):
        """
        Replace symbol in message with batchID
        By default, the symbol is '#'
        """
        message = orig_message.replace(symbol, src_batch_name)
        return message

def uploadBase64ImageToS3(bucket, prefix, _base64):
    obj = s3_resource.Object(bucket,prefix)
    st = obj.put(Body=base64.b64decode(_base64))

    return st

def s3DeleteFile(bucket, location):
    """
    Deletes file from s3 bucket

    Args:
        bucket (string): bucket name
        location (string): location in the bucket

    Returns:
        status (bool): delete status; True/False
    """

    response = s3_client.delete_object(
        Bucket=bucket,
        Key=location,
    )

    loggercolor.warning(f"delete {bucket}/{location} status: {response}")

    return True


def s3DeleteAllObjects(bucket, directory):
    r"""
    Delete all files from location

    Args:
        bucket (String): bucket name
        directory (string): path to directory

    Returns:
        status (bool): delete all objects status. True or False
    """
    # remove the bucket name from directory
    directory = directory.replace(bucket + "/", "")
    logger.debug(f"directory: {directory}")

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)

    try:
        bucket.objects.filter(Prefix=directory).delete()
        logger.info(f"deleted the objects")
        return True
    except Exception as e:
        logger.error(f"s3DeleteAllObjects: Error while deleting s3 objects, {e}")
        return False


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(
            file_name, bucket, object_name, Callback=ProgressPercentage(file_name)
        )
        logger.info(f"s3 write status {response}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def s3UploadDriver(dir: str, filename: str, batchId: str, s3_dir_name: str):
    """
    Concat information to form an object name for
    uploading to s3

    `Arguments are with respect to S3 location`

    Args:
        dir (String): Name of directory
        filename (String): Filename to be uploaded
        batchID (Str/int): InnoBatchID
        s3_dir_name (string): Name of base directory in S3 bucket

    Returns:
        object_name (String): return the string with prefix path from s3
                              Format: s3_dir_name/batchID/dir/filename
    """
    # path = Path()
    object_name = os.path.join(s3_dir_name, str(batchId), dir, filename)
    logger.info(f"Prefix: {object_name}")
    return object_name


def getConfigByStatus(status, cfg):
    """
    Fetch configurations from config object
    according to status Pass/Fail

    params:
        status (String): pass or fail
        cfg (object<Upload>: YAML configuration object)

    returns:
        conf {dict}: returns a dictionary, structure:
                     {
                         "s3_bucket_name": "bucket name",
                         "s3_dir_name": "prefic name",
                     }
    """

    if status == "pass":
        s3_bucket_name = cfg.getOutputBucket()
        s3_dir_name = cfg.getPrefix()

    elif status == "fail":
        s3_info = cfg.getS3BucketFailedInfo()
        logger.info(f"failed metadata {s3_info}")
        s3_bucket_name = s3_info["Bucket"]
        s3_dir_name = s3_info["Prefix"]

    conf = {"s3_bucket_name": s3_bucket_name, "s3_dir_name": s3_dir_name}

    return conf


def s3UploadSingle(filename, filepath, batchID, cfg, flag):
    """
    Upload single file to s3

    Args:
        filename (string): File name as to be uploaded in S3
        filepath (string): Absolute path of image
        batchID (Str/int): InnoBatchID
        cfg (Object<Upload>): YAML info object
        flag (str): flag, 'pass' or 'fail'

    Returns:
        status (bool): Image upload success: True,
                        Image upload failure: False
    """
    # set upload status to True
    upload_status = True

    # fetch config by status
    conf = getConfigByStatus(flag, cfg)
    s3_bucket_name = conf["s3_bucket_name"]
    s3_dir_name = conf["s3_dir_name"]

    object_name = s3UploadDriver("files", filename, batchID, s3_dir_name)

    status = upload_file(
        filepath, s3_bucket_name, object_name
    )

    if status:
        logger.info(f"{filename} upload success..")
    else:
        upload_status = False
        logger.error(f"{filename} upload failed..")

    if upload_status == True:
        return True
    else:
        return False


def s3UploadTrigger(dir_list, batchId, cfg, flag):
    """
    Driver for uploading directory to s3

    Args:
        dir_list [str(directory_url)]: list of directory url
        batchId (int): Current processing batch ID
        cfg (object(Upload)): YAML file configuration object
        flag (str): flag, 'pass' or 'fail'

    returns:
        upload_status (Bool): 'True' or 'False'
    """
    # get config by status
    conf = getConfigByStatus(flag, cfg)
    s3_bucket_name = conf["s3_bucket_name"]
    s3_dir_name = conf["s3_dir_name"]

    # set upload status to True
    upload_status = True

    # iterate over directory
    for dir in dir_list:
        logger.info(f"Walking in directory {dir}...")
        for root, _, files in os.walk(dir):
            for file in files:
                # stdprint(f"root: {root} <-> file: {file}")
                src_path = os.path.join(root, file)
                object_name = s3UploadDriver("files", file, batchId, s3_dir_name)

                status = upload_file(src_path, s3_bucket_name, object_name)

                if status:
                    logger.info(f"{src_path} upload success..")
                else:
                    upload_status = False
                    logger.error(f"{src_path} upload failed..")

    if upload_status == True:
        return "success"
    else:
        return "failure"


def s3UploadJson(JSON_SAVE, jsonObj, cfg, status="", innoBatchId=None, SBIbatchID=None):
    """
    Driver for uploading json to s3

    Args:
        JSON_SAVE (str): s3 upload directory
        jsonObj (dict): json body
        status: _
        batchID (int): Current processing batch ID
        cfg (object): YAML file configuration object

    Returns:
        status [Bool]: 'True' if success and 'False' for failure
    """
    global json_filename_registry

    s3_bucket_name = cfg.getOutputBucket()
    s3_dir_name = SBIbatchID
    # filename = str(innoBatchId)+str('.json')

    filename = str(innoBatchId) + str(status) + str(".json")
    json_filename_registry[innoBatchId] = filename

    prefix = s3UploadDriver(JSON_SAVE, filename, "", s3_dir_name)

    s3_resource = boto3.resource("s3")
    s3_json_obj = s3_resource.Object(bucket_name=s3_bucket_name, key=prefix)

    try:
        # logger.info(f'prefix {prefix}\ns3_bucket_name {s3_bucket_name}')
        logger.info(f"json body: {jsonObj}")
        s3_json_obj.put(Body=json.dumps(jsonObj))
        return {"Status": "SUCCESS", "Bucket": s3_bucket_name, "Prefix": prefix}

    except Exception as e:
        logger.error(f"excepion in json upload: {e}")
        return {"Status": "FAILURE", "Bucket": s3_bucket_name, "Prefix": prefix}
