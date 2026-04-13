import os
import json
import numpy as np
import base64
from base64 import decodebytes

from .utils.s3_utility.s3_data_fetch import s3Pparser
from .utils import constants as const
from .utils.utils import objToBase64, cv2ToBase64

from modules.logger.logger import LoggerInit
from src.utils.kms.encrypt import KeyEncrypt
from src.utils.rsa_crypto import decryption

kms_obj = KeyEncrypt()
# initiate logger
module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=False)


def extract_info_from_path(path):
    path_split = path.split("/")
    filename = os.path.basename(path)
    bucket = path_split[0]

    if len(path_split) > 2:
        prefix = "/".join(path_split[1:-1])
    else:
        prefix = ""

    return bucket, prefix, filename


def fetch_and_transform(path, encrypted_content, aes_encrypted_key):

    if encrypted_content:
        bucket, prefix, filename = extract_info_from_path(path)
        filename_enc = filename + ".enc"
        outdir_abs_filepath = os.path.join(const.LOCAL_TEMP_FILE_SAVE_LOC, filename)
        outdir_abs_filepath_enc = os.path.join(
            const.LOCAL_TEMP_FILE_SAVE_LOC, filename_enc
        )

        # fetch from s3
        download_status = s3Pparser(
            bucket_name=bucket,
            prefix=os.path.join(prefix, filename_enc),
            outdir=outdir_abs_filepath_enc,
        )

        if download_status:
            # read from disk
            with open(outdir_abs_filepath_enc, "r") as b:
                r = b.read()

            # decrypt it
            d = decryption.decryption_driver(r, kms_obj, aes_encrypted_key)

            # save to disk
            with open(outdir_abs_filepath, "wb") as output_file:
                output_file.write(decodebytes(d))

            # delete encrypted file
            os.remove(outdir_abs_filepath_enc)

            # convert to base64 form path
            base64_out = objToBase64(outdir_abs_filepath)
            os.remove(outdir_abs_filepath)
            logger.info("S3 download object Successful")
            return base64_out, True

        logger.warning("Download object failed from s3")
        return None, False

    bucket, prefix, filename = extract_info_from_path(path)
    outdir_abs_filepath = os.path.join(const.LOCAL_TEMP_FILE_SAVE_LOC, filename)

    # fetch from s3
    download_status = s3Pparser(
        bucket_name=bucket,
        prefix=os.path.join(prefix, filename),
        outdir=outdir_abs_filepath,
    )

    if download_status:
        # read from disk
        with open(outdir_abs_filepath, "rb") as b:
            r = b.read()

        # convert to base64 form path
        base64 = objToBase64(outdir_abs_filepath)
        os.remove(outdir_abs_filepath)
        logger.info("S3 download object Successful")
        return base64, True
    else:
        logger.warning("Download object failed from s3")

    return None, False


def remove_special_chars(text):
    special_char = ["`", "!", "@", "#", "&", "$", "'", ">", "<"]

    for spch in special_char:
        try:
            if spch in text:
                text = text.replace(spch, "")
        except:
            continue
    return text


def trim_length(text, trim_length=250):
    if len(text) > trim_length:
        return text[:trim_length]

    return text


def process(record, aes_encrypted_key):
    print()
    print(record)
    attachment_details = record["attachments"]

    for obj in attachment_details:
        mask_status = obj["is_masked"]

        if mask_status:
            if "file" in obj:
                filecontent_base64, status = fetch_and_transform(
                    obj["file"], True, aes_encrypted_key
                )

                if status:
                    obj["file"] = filecontent_base64
                else:
                    logger.error(
                        f'File could not be downloaded from s3: {obj["file"]}'
                    )
                    del obj["file"]

    return record


def download_and_process(record):
    """
    {"UpdateDocMaskingStatusRq": {"RequestType": "Interaction",
    "RequestRowId": "1-40BYN1XP", "RequestID": "7443715695193087509",
      "AttachmentsList": {"AttachmentDetails": []}}}
    """

    global r
    empty = False
    operations = {"message": "no opearation performed/ unable to fetch operations"}

    if type(record) == str:
        record = json.loads(record)

    if "Items" in record:
        record = record["Items"][0]

    metadata = {"transactionId": record["PK"].split("#")[1]}

    if not "RAW" in record:
        print("Not sent to webhook")
        return json.dumps(record), metadata, True, operations

    """
    if type(record['RAW']) == str:
        raw = json.loads(record['RAW'])
    else:
        raw = record['RAW']
    """

    print("Encrypted raw: ", record["RAW"])
    # record['RAW'] = json.loads(record['RAW'])['content']

    # get the encrypted aes key
    aes_encrypted_key = record["AES_KEY_ENCRYPTED"]
    aes_encrypted_key = base64.b64decode(aes_encrypted_key)
    # decrypt the key and content
    # raw_decrypted = decryption.decryption_driver(record['RAW'], kms_obj, aes_encrypted_key)

    # print('raw_decrypted: ',raw_decrypted)
    # raw = raw_decrypted.decode('utf-8')
    # print("raw: ", type(raw))

    # if type(record['RAW']) == str:
    #     raw = json.loads(raw)
    # else:
    #     raw = record['RAW']
    raw = record["RAW"]
    print("raw: ", raw)

    # if type(raw) == list:
    #         processed_data, operations = process_old(raw, record, aes_encrypted_key)
    # else:
    processed_data = process(raw, aes_encrypted_key)
    print("processed_data: ", processed_data)
    if not processed_data.get("attachments", []):
        empty = True
    return json.dumps(processed_data), metadata, empty, operations
