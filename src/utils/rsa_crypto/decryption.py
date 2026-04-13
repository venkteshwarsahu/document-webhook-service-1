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

def decrypt_aes_key(private_key, encrypted_aes_key):
    encrypted_aes_key = base64.b64decode(encrypted_aes_key)
    aes_key = private_key.decrypt(
        encrypted_aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return aes_key

def decrypt_payload(aes_key, encrypted_payload):
    encrypted_payload = base64.b64decode(encrypted_payload)
    iv = encrypted_payload[:16]
    encrypted_payload = encrypted_payload[16:]
    cipher = Cipher(algorithms.AES(aes_key), modes.CFB(iv))
    decryptor = cipher.decryptor()
    decrypted_payload = decryptor.update(encrypted_payload) + decryptor.finalize()
    return decrypted_payload

def decryption_driver(encrypted_payload, kms_obj, encrypted_aes_key):
    # Load RSA keys

    print('encrypted_aes_key: ', encrypted_aes_key)
    # Decrypt AES key with RSA
    decrypted_aes_key = kms_obj.decrypt(encrypted_aes_key)
   
    print('decrypted_aes_key: ', decrypted_aes_key)
    # Decrypt JSON payload
    decrypted_payload = decrypt_payload(decrypted_aes_key, encrypted_payload)

    return decrypted_payload

