import os
import base64
import requests
import time
import binascii
import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from .utils.utils import Config, getDictionaryPhysicalSize
from .utils.constants import DEFAULT_CONFIG_FILE
from modules.logger.logger import LoggerInit

import yaml

module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=False)

# load webhook config
response_config = Config(DEFAULT_CONFIG_FILE)


def pretty_print_GET(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in
    this function because it is programmed to be pretty
    printed and may differ from the actual request.
    """
    print(
        "{}\n{}\r\n{}\r\n\r\n{}".format(
            "-----------START-----------",
            req.method + " " + req.url,
            "\r\n".join("{}: {}".format(k, v) for k, v in req.headers.items()),
            req.body,
        )
    )


class Configurations:
    config = {}

    @staticmethod
    def getOauthUrl():
        if not Configurations.config:
            raise Exception("webhook config not set")

        return Configurations.config["OAUTH2_URL"]

    @staticmethod
    def getOauthUrl():
        if not Configurations.config:
            raise Exception("webhook config not set")

        return Configurations.config["OAUTH2_URL"]

    @staticmethod
    def getOauthBase64Pass():
        if not Configurations.config:
            raise Exception("webhook config not set")

        client_id = Configurations.config["CLIENT_ID"]
        client_secret = Configurations.config["CLIENT_SECRET"]
        base64_encoded = base64.b64encode(
            bytes(f"{client_id}:{client_secret}", "utf-8")
        ).decode()
        return base64_encoded

    @staticmethod
    def getSigningKey():
        if not Configurations.config:
            raise Exception("webhook config not set")

        signing_key = Configurations.config["SIGNING_KEY"]
        return signing_key

    @staticmethod
    def getWebhookUrl():
        if not Configurations.config:
            raise Exception("webhook config not set")

        webhook_url = Configurations.config["WEBHOOK_URL"]
        return webhook_url


class CryptoHelper:
    BLOCKSIZE = 16

    @staticmethod
    def encrypt(data):
        data = bytes(data, "utf-8")
        signing_key = Configurations.getSigningKey()
        cipher = AES.new(binascii.unhexlify(signing_key), AES.MODE_ECB)
        ciphertext = cipher.encrypt(pad(data, CryptoHelper.BLOCKSIZE))
        return binascii.hexlify(ciphertext)

    @staticmethod
    def decrypt(encypted_data_hex):
        signing_key = Configurations.getSigningKey()
        cipher = AES.new(binascii.unhexlify(signing_key), AES.MODE_ECB)
        padded_decrypted_text = cipher.decrypt(binascii.unhexlify(encypted_data_hex))
        return unpad(padded_decrypted_text, CryptoHelper.BLOCKSIZE).decode()


class OAuthHelper:
    @staticmethod
    def getToken():
        logger.info("Requesting for auth token")
        oauth_url = Configurations.getOauthUrl()
        oauth_auth = Configurations.getOauthBase64Pass()
        logger.info(f"oauth url: {oauth_url}")
        # resp = requests.post(
        #     oauth_url, headers={"Authorization": f"Basic {oauth_auth}"}
        # )
        resp = {
            "access_token": "OAuth token",
            "expires_in": 3600
        }
        logger.info(f"OAUTH Resp: {resp.json()}")
        return resp.json()


class Webhook:
    def __init__(self, config=None):
        if config:
            Configurations.config = config
        else:
            Configurations.config = response_config.getConfigAll()

        # oauth_resp = OAuthHelper.getToken()
        oauth_resp = {"access_token": "OAuth token", "expires_in": 3600}
        self.oauth_token = oauth_resp["access_token"]
        self.oauth_expires_in = oauth_resp["expires_in"]

    def sendData(self, metadata, content, payload=None):

        # get webhook url
        # webhook_url = Configurations.getWebhookUrl()
        webhook_url = "Webhook url"
        # encrypt data/content
        encrypted_hex_bytes = CryptoHelper.encrypt(content)
        # get checksum
        checksum = binascii.crc32(encrypted_hex_bytes)
        # create payload

        if payload is None:
            payload = {
                "metadata": metadata,
                "checksum": str(checksum),
                "encryptedContent": encrypted_hex_bytes.decode(),
            }

        # logger.info(f"Webhook send payload, {payload}")

        oauth_token = self.oauth_token
        oauth_expires_in = self.oauth_expires_in

        # TODO: check if oauth token is valid or not based on timeout/ttl

        # send post request
        webhook_time_start = time.time()
        logger.info(f"webhook url: {webhook_url}")

        # =================REQUEST=======================
        # print(payload)        #resp = requests.get(
        #    webhook_url,
        #    json=payload,
        #    headers={"Authorization": f"Bearer {oauth_token}"},
        # )
        logger.info(f"token: {oauth_token}")
        req = None
        # req = requests.Request(
        #     "PUT",
        #     url=webhook_url,
        #     json=payload,
        #     headers={"Authorization": f"Bearer {oauth_token}"},
        # )
        # prepared = req.prepare()
        # s = requests.Session()
        # resp = s.send(prepared)

        webhook_time_end = time.time()
        # =============================================
        logger.debug("Webhook Response: ", resp)
        logger.debug(f"Webhook response content: {resp.content}")
        logger.info(
            "Time taken to send request: ", (webhook_time_end - webhook_time_start)
        )

        try:
            _resp = resp.json()
        except:
            raise Exception(
                f"Unable to parse response body as json, response status code: {resp.status_code}. message: {str(resp.content)} || {getDictionaryPhysicalSize(payload)} MB"
            )
        if resp.status_code == 401:
            # relogin
            # oauth_resp = OAuthHelper.getToken()
            oauth_resp = {"access_token": "OAuth token", "expires_in": 3600}
            self.oauth_token = oauth_resp["access_token"]
            self.oauth_expires_in = oauth_resp["expires_in"]
            self.sendData(metadata, content)

        elif "code" in _resp.keys() or resp.status_code != 200:
            logger.error(f"SBI Webhook error")
            logger.info(f"Response: {resp.json()}")
            raise Exception(f"{resp.json()}")

        print("Webhook response: ", _resp)
        encrypted_content = _resp["encryptedContent"]
        checksum = _resp["checksum"]
        payload_size = getDictionaryPhysicalSize(payload)
        # verify checksum
        # if binascii.crc32(bytes(encrypted_content, "utf-8")) != checksum:
        #     raise Exception("sbi response checksum match error")

        decrypted_content = CryptoHelper.decrypt(encrypted_content)

        logger.info(f"Decrypted content, {decrypted_content}")

        return decrypted_content, payload_size
        # decrypt resp
