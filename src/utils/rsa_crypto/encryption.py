import json
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
import os

def load_private_key(pem_file_path):
    with open(pem_file_path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    return private_key

def load_public_key(pem_file_path):
    with open(pem_file_path, 'rb') as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read()
        )
    return public_key

def encrypt_aes_key(public_key, aes_key):
    encrypted_aes_key = public_key.encrypt(
        aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return base64.b64encode(encrypted_aes_key).decode('utf-8')

def encrypt_payload(aes_key, payload):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(aes_key), modes.CFB(iv))
    encryptor = cipher.encryptor()
    encrypted_payload = encryptor.update(payload.encode('utf-8')) + encryptor.finalize()
    return base64.b64encode(iv + encrypted_payload).decode('utf-8')

def encrypt_driver(payload:str, kms_obj, aes_key:bytes):
    # Load RSA keys
    print("AES Key: ", aes_key, type(aes_key))
    # decrypted_aes_key = kms_obj.decrypt(aes_key)
    # print(f"decrypted AES Key: {decrypted_aes_key}, {type(decrypted_aes_key)}")
    print('payload type: ', type(payload))
    encrypted_payload = encrypt_payload(aes_key, payload)
    print(f"Encrypted Payload: {encrypted_payload}")

    encrypted_aes_key = kms_obj.encrypt(aes_key)

    return encrypted_payload, encrypted_aes_key

