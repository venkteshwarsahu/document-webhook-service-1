import boto3
from botocore.exceptions import ClientError

class KeyEncrypt:
    def __init__(self):
        self.kms_client = boto3.client('kms')

    def encrypt(self, text, key_id = "arn:aws:kms:ap-south-1:897570523132:key/02e4d646-ab9c-4bb8-8121-67ea1114fcfe"):
        """
        Encrypts text by using the specified key.

        :param key_id: The ARN or ID of the key to use for encryption.
        :return: The encrypted version of the text.
        """
        
        print('text to encrypt: ')
        print(text)
        try:
            cipher_text = self.kms_client.encrypt(
                KeyId=key_id, Plaintext=text
            )["CiphertextBlob"]

        except ClientError as err:
            print(
                "Couldn't encrypt text. Here's why: %s",
                err.response["Error"]["Message"],
            )
        else:
            print(f"Your ciphertext is: {cipher_text}")
            return cipher_text

    def decrypt(self, cipher_text, key_id = "arn:aws:kms:ap-south-1:897570523132:key/02e4d646-ab9c-4bb8-8121-67ea1114fcfe"):
        """
        Encrypts text by using the specified key.

        :param key_id: The ARN or ID of the key to use for encryption.
        :return: The encrypted version of the text.
        """

        try:
            print(f'type of payload: ', type(cipher_text))
            text = self.kms_client.decrypt(
                KeyId=key_id, CiphertextBlob=cipher_text
            )["Plaintext"]
            
            print(text)
            return text
        except ClientError as err:
            print("Couldn't decrypt your ciphertext. Here's why: ", err)
        else:
            print(f"Your plaintext is {text.decode()}")
