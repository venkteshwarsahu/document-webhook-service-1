import os

DEFAULT_CONFIG_FILENAME = 'webhook_handler_config_prod.yaml'
CONFIG_SAVE_PREFIX = 'src/config/'
DEFAULT_CONFIG_FILE = os.path.join(CONFIG_SAVE_PREFIX, DEFAULT_CONFIG_FILENAME)

# S3
CONFIG_BUCKET_NAME = "pdfmaskconfig"
CONFIG_PREFIX = "configs"

LOCAL_TEMP_FILE_SAVE_LOC = 'temp'
