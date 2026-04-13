"""
1. Fetch the processed records
2. Prepare the output
3. Send output to webhook
4. Update status to database
5. Retry for 3 times in case of failure
6. If all process if done, generate and mail daily MIS
"""

import os
import sys
import json
import copy
import time
from decimal import Decimal

from src.utils.utils import (
    Directory,
    Data,
    Config,
    Downloader,
    getDictionaryPhysicalSize,
)
from src.db.connection import DBClient
from src.records import get_records_by_status, update_records
from src.process_records import download_and_process
from src.webhook_driver import Webhook
from src.utils import constants as const
from src.generate_report import generate_daily_mis
from src.mailer_driver import send_mail
from src.sqs.sqs_metadata_fetch import sqs_worker, deleteFromSqs, addToSQSFifo
from src.utils.config_fetcher import (
    get_metadata_database,
    get_billing_database,
    get_sqs,
    get_sqs_dlq,
    getTotalRetries,
    getFunctionalError,
)


from modules.logger.logger import LoggerInit
import datetime


# initiate logger
module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=False)

# initiate directory obj
directory = Directory([])
# create directories
directory.createDefault()

data = Data()

# CONFIG FILE DEFAULTS AS PER ENV
CONFIG_FILE_NAME = (
    "webhook_handler_config_test.yaml"
    if "test" in sys.argv
    else "webhook_handler_config_prod.yaml"
)

# fetch configuration file from s3
config_download_status = data.fetchConfigFromS3(
    const.CONFIG_BUCKET_NAME,
    os.path.join(const.CONFIG_PREFIX, CONFIG_FILE_NAME),
    os.path.join(const.CONFIG_SAVE_PREFIX, CONFIG_FILE_NAME),
)

logger.info(f"Config download status: {config_download_status}")

cfg = Config(os.path.join(const.CONFIG_SAVE_PREFIX, CONFIG_FILE_NAME))
# fetch configs from s3
cfg_all = cfg.getConfigAll()

db_metadata = get_metadata_database(config=cfg_all)
db_billing = get_billing_database(config=cfg_all)
output_sqs = get_sqs(config=cfg_all)
output_sqs_dlq = get_sqs_dlq(config=cfg_all)
total_allowed_retries = getTotalRetries(config=cfg_all)
functional_errors = getFunctionalError(config=cfg_all)

# print(functional_errors)
# create db client
logger.debug("database: ", db_metadata[0], db_metadata[1])
db_client = DBClient(db_metadata[0], db_metadata[1])
db_client_billing = DBClient(db_billing[0], db_billing[1])

# initiate webhook
webhook = Webhook()


def submit_and_record(record, metadata, processed_record):
    try:

        date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        webhook_response, payload_size = webhook.sendData(
            metadata=metadata, content=processed_record
        )

        try:
            record["PAYLOAD_SIZE"] = Decimal(str(payload_size))
        except Exception:
            record["PAYLOAD_SIZE"] = Decimal(0)

        logger.info(f"Webhook response: {webhook_response}")
        logger.info(f"METADATA for above response: {metadata}")
        logger.info(f"Payload size: {payload_size} MB")

        db_status_meta = update_records(
            record=record,
            update_for="success",
            db="METADATA",
            db_obj=db_client,
            api_response=webhook_response,
            send_date=date,
        )
        logger.info(f"Database update for metadata: {db_status_meta}")

        return "SUCCESS", 0, webhook_response

    except Exception as e:
        logger.error(f"Exception in webhook handler... {e}")
        logger.info(f"METADATA for above response: {metadata}")
        payload_size = str(e).split("||")[-1].strip(" MB") if "||" in str(e) else "0"
        try:
            record["PAYLOAD_SIZE"] = Decimal(str(payload_size))
        except Exception:
            record["PAYLOAD_SIZE"] = Decimal(0)
        # metadata failure
        record["RETRY"] = int(record["RETRY"]) + 1
        db_status_meta = update_records(
            record=record,
            update_for="failure",
            db="METADATA",
            db_obj=db_client,
            api_response=str(e),
            send_date=date,
        )
        logger.info(
            f"Database update on webhook failure for metadata: {db_status_meta}"
        )

        return "FAILURE", int(record["RETRY"]), str(e)


def driver():
    global total_allowed_retries
    while 1:

        record = sqs_worker(output_sqs)
        print("record: ", record)

        if "RecivedList" in record:
            record = json.loads(record["RecivedList"][0])
            record_cpy = copy.deepcopy(record)

            if "Items" in record:
                record = record["Items"][0]

        else:
            logger.info("No data in queue")
            continue

        # process record

        processed_record, metadata, is_empty, operations = download_and_process(record)
        try:
            # processed_record, metadata, is_empty, operations = download_and_process(record)
            if not "OPERATIONS" in record:
                print("appended operations to process")
                record["OPERATIONS"] = operations

            if not "RETRY" in record:
                record["RETRY"] = 0

            if is_empty:
                continue

        except Exception as e:
            logger.error(f"Exception in processing record: {e}")

            # metadata pending
            db_status_meta = update_records(
                record=record,
                update_for="pending",
                db="METADATA",
                db_obj=db_client,
                api_response=f"Pending due to error: {e}",
            )

            continue

        # send to webhook
        current_retry = int(record["RETRY"])
        logger.debug(f"total_allowed_retries: {total_allowed_retries}")
        logger.debug(f"current_retry: {current_retry}")
        dlq_save = False
        while total_allowed_retries - current_retry > 1:
            logger.info(f"Retrying ...")
            logger.info(f"Retry remaining: {total_allowed_retries - current_retry}")
            temp_processed_records = json.loads(processed_record)
            new_payload = {
                "transactionID": record.get("TRANSACTION_ID", ""),
                "requestID": record["PK"].split("#")[1],
                "status": "success",
                "aadhaar_status": record.get("AADHAAR_STATUS", "not_found"),
                "attachments": temp_processed_records.get("attachments", []),
            }

            print("New Payload")
            print("*************************************************")
            print(new_payload)
            print("Webhook Calling...")
            print("*************************************************")
            status, current_retry, message = "SUCCESS", 0, "Success"

            # status, current_retry, message = submit_and_record(
            #     record=record, processed_record=processed_record, metadata=metadata
            # )

            if status == "SUCCESS":
                logger.info("Submitted to webhook")
                break
            elif status == "FAILURE":
                if str(message) in functional_errors:
                    logger.info(
                        f"Failed to submit to webhook due to functional error {message}"
                    )
                    dlq_save = False
                    break
                else:
                    logger.info(f"Failed to submit the record")
                    dlq_save = True

        if total_allowed_retries - current_retry <= 2 and dlq_save:
            # push record to dlq
            logger.info(f"Sending failed record to dlq, {output_sqs_dlq}")
            resp = addToSQSFifo(json.dumps(record_cpy), output_sqs_dlq)

        print("\n")


if __name__ == "__main__":
    print("Starting new document webhook service....")
    driver()
# source "/Users/venktesh/Documents/Innodeed/Documents Processing/inno-document-pipeline-service/.venv/bin/activate"
