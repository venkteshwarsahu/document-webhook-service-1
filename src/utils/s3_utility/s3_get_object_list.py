import boto3
import os

from .s3_data_fetch import parseFromList

def fetchObjectListFromS3(bucket, prefix, subdir):
    """
        fetch object from s3
    """
    subdirs = ['train','val', subdir]

    data = {}
    counter = 1
    my_bucket = s3.Bucket(bucket)
    for datatype in subdirs:
        try:
            temp = []
            for obj in my_bucket.objects.filter(Prefix=os.path.join(prefix, datatype)):
                # temp.append(os.path.join(bucket, obj.key))
                temp.append(obj.key)
                print(f"{counter}. {obj.key} appended..")
                counter+=1
        except Exception as e:
            print(f'error: {e}')

        data[datatype] = temp
    
    return data

def loadTrainDataFromS3(bucket, 
                        prefix_base, 
                        savedir,
                        prefix_folder,
                        s3_object = None):
    """
        this method initialize the boto object and retreve object list
    """
    global s3
    if s3_object == None:
        s3 = boto3.resource('s3')
    else:
        s3 = s3_object
    
    print(s3)
    data = fetchObjectListFromS3(bucket, prefix_base, prefix_folder)

    for key in data:
        status = parseFromList(bucket, data[key], os.path.join(savedir, key))
    
    return status
