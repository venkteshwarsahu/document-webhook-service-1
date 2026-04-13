from .constants import DEFAULT_CONFIG_FILE

def get_mail_config(config_file=None):
    if config_file is None:
        config_file = DEFAULT_CONFIG_FILE

def get_metadata_database(config):
    db_all = config['DATABASE']
    meta_db = db_all['METADATA']
    db_main = meta_db['DB']
    db_gsi = meta_db['DB_GSI']

    return db_main, db_gsi

def get_billing_database(config):
    db_all = config['DATABASE']
    meta_db = db_all['BILLING']
    db_main = meta_db['DB']
    db_gsi = meta_db['DB_GSI']

    return db_main, db_gsi

def get_sqs(config):
    sqs = config['SQS']['OUTPUT']
    return sqs

def get_sqs_dlq(config):
    sqs = config['SQS']['DLQ']
    return sqs

def getTotalRetries(config):
    total_retries = config['RETRIES']
    return total_retries

def getFunctionalError(config):
    functional_errors = config['ERRORS_TO_SKIP']
    return functional_errors