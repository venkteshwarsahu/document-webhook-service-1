import boto3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    '--table_name', choices=['mask_table', 'mask_table_woi','job_table', 'mask_table_prod', 'ocr_table', 'innobatch_lambda', 'mask_table_prod_woi'], default=None)
parser.add_argument(
    '--delete_table', default=None)

args = parser.parse_args()


def delete_table_function(table_name, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    print("deleting table: ", table_name)
    table = dynamodb.Table(table_name)
    table.delete()

def create_mask_table_prod_without(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='Aadhar_Mask_OCR',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def create_mask_table_woi(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='Aadhar_Mask_OCR_test',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


def create_mask_table_prod(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='Aadhar_Mask_OCR',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'batch',
                'KeyType': 'RANGE'  # Primary key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'batch',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'batch-GSI',
                'KeySchema': [
                    {
                        'AttributeName': 'batch',
                        'KeyType': 'HASH'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def create_mask_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='Aadhar_Mask_OCR_test',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'batch',
                'KeyType': 'RANGE'  # Primary key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'batch',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'batch-GSI',
                'KeySchema': [
                    {
                        'AttributeName': 'batch',
                        'KeyType': 'HASH'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


def create_job_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource(
            'dynamodb')

    table = dynamodb.create_table(
        TableName='SB_Instance_Table_test',
        KeySchema=[
            {
                'AttributeName': 'instanceId',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'innoBatchID',
                'KeyType': 'RANGE'  # Primary key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'instanceId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'innoBatchID',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'innoBatchID-GSI',
                'KeySchema': [
                    {
                        'AttributeName': 'innoBatchID',
                        'KeyType': 'HASH'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def create_innobatch_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='InnoBatch_lambda_test',
        KeySchema=[
            {
                'AttributeName': 'sb_batch',
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sb_batch',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def create_ocr_rawtext_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.create_table(
        TableName='OCR_rawtext_test',
        KeySchema=[
            {
                'AttributeName': 'name',
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'name',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

if __name__ == '__main__':
    table_name = args.table_name
    delete_table = args.delete_table

    if delete_table != None:
        delete_table_function(delete_table)

    if table_name == "mask_table":
        mask_table = create_mask_table()
        print("Table status:", mask_table.table_status)

    if table_name == "mask_table_woi":
        mask_table = create_mask_table_woi()
        print("Table status:", mask_table.table_status)

    if table_name == "mask_table_prod":
        mask_table_prod = create_mask_table_prod()
        print("Table status:", mask_table_prod.table_status)

    if table_name == "mask_table_prod_woi":
        mask_table_prod_without = create_mask_table_prod_without()
        print("Table status:", mask_table_prod_without.table_status)

    if table_name == "job_table":
        job_table = create_job_table()
        print("Table status:", job_table.table_status)
    
    if table_name == "ocr_table":
        ocr_table = create_ocr_rawtext_table()
        print("Table status:", ocr_table.table_status)
    
    if table_name == "innobatch_lambda":
        batch_table = create_innobatch_table()
        print("Table status:", batch_table.table_status)
