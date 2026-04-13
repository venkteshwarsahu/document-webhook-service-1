import os
import sys
import boto3
import json
from modules.logger.logger import LoggerInit

module_name = os.path.basename(__file__)
logger = LoggerInit(__file__, color=False)

s3 = boto3.client('s3')
s3_res = boto3.resource("s3")
opdir = "batch"

def bucketNameParser(full_s3_path):
    s3_bucket = full_s3_path.split('/')[0]
    return s3_bucket

def s3Pparser(bucket_name, prefix, outdir):
    try:
        logger.info(f'Downloading file {prefix} to {outdir}')
        s3.download_file(bucket_name, prefix, outdir)
        return True
    except Exception as e:
        logger.error(f"Exception in downloading object from s3 {e}")
        return False

def fetchBatchFromS3(metadata_sqs):
    print("Starting fatching batch from s3 bucket")
    try:
        for jobj in metadata_sqs:
            
            #DEBUG LOG
            print(f'jobj: {jobj}')
            # jobj = json.loads(obj)
            s3_bucket = bucketNameParser(jobj["image_S3_path"])
            BATCH = jobj["image_S3_path"].split('/')[1]
            prefix = os.path.join(BATCH, jobj['name'])
            outdir = os.path.join(opdir,jobj["name"] )
            status = s3Pparser(s3_bucket, prefix, outdir)

            if status:
                print(f"get {prefix} success")
            else:
                print(f"get {prefix} failure")

        print(f"output directory: {opdir}")
        return True
    
    except Exception as e:
        print(f"exception {e}")
        return False
    
def fetchJsonFromS3(bucket, file_key_name):
    """
        get json from s3 bucket

        Args:
            bucket (str): s3 bucket name
            file_key_name (str): absolute location in s3 buket
        
        Returns:
            file_content {dict}: returns the fetched json body
    """
    # get resource object
    s3_src_json_obj = s3_res.Object(bucket, file_key_name)
    # getting json object
    file_content = s3_src_json_obj.get()["Body"].read().decode("utf-8")

    return file_content

def listS3(bucket, directory):
    """
        list objects in s3

        Args:
            bucket (string): bucket name
            directory (string): directory
        
        Results:
            objects [list]: list of objects
    """
    my_bucket = s3_res.Bucket(bucket)
    temp = []
    try:
        for obj in my_bucket.objects.filter(Prefix=os.path.join(directory)):
            # temp.append(os.path.join(bucket, obj.key))
            temp.append(obj.key)
    except Exception as e:
        logger.error(f'Exception in fetching objects: {e}')

    return temp