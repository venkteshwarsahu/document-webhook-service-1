import os
import shutil
import pathlib
import boto3
import yaml
from Naked.toolshed.shell import execute_js, muterun_js
from modules.logger.logger import LoggerInit
import json
from datetime import datetime
from imageio import imread
from random import randint
import base64
import io
import sys
import cv2


s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
module_name = os.path.basename(__file__)
logger = LoggerInit(module_name, color=False)

show_stdprint = True

def getTodaysDate():
    ts = datetime.now()

    date = ts.day
    month = ts.month
    year = ts.year

    return date, month, year

def getRandomId(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    uidd = randint(range_start, range_end)

    dt_obj = datetime.now()
    uid = dt_obj.timestamp() * 1000
    return int(uid)+int(uidd)

def base64ToCV2(base64_string):
    """
        This method is responsible for converting base64 image string into cv2 image
    """
    img = imread(io.BytesIO(base64.b64decode(base64_string)))
    cv2_img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

    return cv2_img

def getDictionaryPhysicalSize(dictionary):
    """returns the physical size of the dictionary in mb"""
    if isinstance(dictionary, str):
        return sys.getsizeof(dictionary) / (1024 * 1024)
    total_size = 0
    for key, value in dictionary.items():
        total_size += sys.getsizeof(value)  
    return total_size / (1024 * 1024)


def objToBase64(object_location):
    with open(object_location, "rb") as _file:
        encoded_string = base64.b64encode(_file.read())
    
    return encoded_string.decode()

def cv2ToBase64(cv2_image):
    retval, buffer = cv2.imencode('.jpg', cv2_image)
    jpg_as_text = base64.b64encode(buffer)

    return jpg_as_text

def getSubstringCount(str, substr):
    """Substring count"""
    return str.count(substr)


def decodeFrame(fullpath, pagenum, filename):
    response = muterun_js(f"./server/app.js {fullpath} {pagenum} {filename}")
    if response.exitcode == 0:
        result = response.stdout.decode("utf-8")
        result = json.loads(result)
        return result
    else:
        # print(response.stderr.decode("utf-8"))
        logger.error("Error in executin javascript recovery....")
        return {"Status": "ERROR", "Message": response.stderr.decode("utf-8")}


class Directory:
    def __init__(self, dirlist):
        if type(dirlist) != list:
            raise RuntimeError(f"List is required, {type(dirlist)} found...")
        self.dirlist = dirlist

    def checkIfDirExists(self, path):
        isdir = os.path.isdir(path)
        return isdir

    def removeFile(self, filepath):
        status = {}
        try:
            os.remove(filepath)
            status["status"] = True
            status["message"] = "success"

            logger.info(f"{filepath} File removed successfully")
            return status
        except FileNotFoundError as e:
            logger.info("File not exist so not removed")
            status["status"] = False
            status["message"] = str(e)

            return status

    def create(self, dirlist=None):
        if dirlist == None:
            dirlist = self.dirlist

        success_flag = True
        for directory in dirlist:
            if self.checkIfDirExists(directory):
                # logger.info(f"{directory} already exists")
                continue

            logger.info(f"creating directory {directory}")
            try:
                pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
                logger.info("created")
            except Exception as e:
                success_flag = False
                logger.error(f"{module_name} failed creating directory, {e}")
                # logger.info(f"Exception in {module_name}: {e}")

        return success_flag

    def remove(self, dirlist=None):
        if dirlist == None:
            logger.info("Given no directory.. removing all")
            dirlist = self.dirlist

        success_flag = True

        for directory in dirlist:
            if not self.checkIfDirExists(directory):
                logger.info(f"{directory} not exists")
                continue

            logger.info(f"removing {directory}")
            try:
                shutil.rmtree(directory)
                logger.info(f"removed..")
            except Exception as e:
                success_flag = False
                logger.error(f"Exception in {module_name}: {e}")

        return success_flag

    def setDefault(self):
        dirlist = [
            "./temp"
        ]

        return dirlist

    def createDefault(self):
        dirlist = self.setDefault()
        status = self.create(dirlist)

        if status:
            logger.info("All Directory successfully created")
            return status

        return status

    def removeDefault(self):
        dirlist = self.setDefault()
        status = self.remove(dirlist)

        if status:
            logger.info("All Directory successfully removed")
            return status

        return status


class Downloader:
    def __init__(self, bucket_name, prefix, outdir):
        global s3
        try:
            self.status = {}
            print("Type of outdir", outdir, "Type of prefix", prefix)
            if type(outdir) == list and type(prefix) == list:
                print("Looping through lists")
                for _outdir, _prefix in zip(outdir, prefix):
                    print("S3 DOWNLOADS: ", _outdir, _prefix)
                    if os.path.isfile(_outdir):
                        raise Exception("FileAleradyExists")
                    s3.download_file(bucket_name, _prefix, _outdir)
                    e = None
                    self.status["status"] = True
                    self.status["Exception"] = e
                return
            if os.path.isfile(outdir):
                raise Exception("FileAleradyExists")
            s3.download_file(bucket_name, prefix, outdir)
            e = None
            self.status["status"] = True
            self.status["Exception"] = e

        except Exception as e:
            logger.error(f"Exception in {module_name}: {e}")

            self.status["status"] = False
            self.status["Exception"] = str(e)

    def getStatus(self):
        return self.status

class Config:
    config = None
    config_path = None

    def __init__(self, config_file):
        if Config.config is not None:
            self.config = Config.config
            logger.info("Using cached config file object..")
        else:
            config_loader = open(config_file, "r")
            self.config = yaml.load(config_loader, Loader=yaml.FullLoader)
            logger.info(f"Config file loaded..")
            Config.config = self.config
            Config.config_path = config_file
            logger.info(f"Caching file object")

    def getConfig(self, cfg):
        return self.config[cfg]

    def getConfigAll(self):
        return self.config


class GetFormType(Config):
    def __init__(self, config_file):
        cfg_obj = Config(config_file)
        self.formDetectTypes = cfg_obj.getConfig("FORM_DETECT_TYPES")

    def getNames(self):
        names = self.formDetectTypes["NAMES"]
        return names

    def getNos(self):
        nos = self.formDetectTypes["NOS"]
        return nos

    def getMinScores(self):
        default_score_name = 75
        default_score_nos = 75
        try:
            min_score_name = self.formDetectTypes["MIN_SCORE_NAMES"]
            min_score_nos = self.formDetectTypes["MIN_SCORE_NOS"]
            return (min_score_name, min_score_nos)
        except Exception as e:
            logger.warning(f"Error Occurs while getting getMinScores: {e}")
            logger.info(
                f"Returning Default values of: {default_score_name}, {default_score_nos}"
            )
            return (default_score_name, default_score_nos)


class GetAzureConfig(Config):
    def __init__(self, config_file):
        cfg_obj = Config(config_file)
        self.azureConfig = cfg_obj.getConfig("AZURE_CONFIG")

    def securityConfig(self):
        endpoint = self.azureConfig["ENDPOINT"]
        apikey = self.azureConfig["API_KEY"]
        return (endpoint, apikey)

    def getAzureWait(self):
        waittime = self.azureConfig["WAIT_TIME"]
        return waittime

class GetOCRConfig(Config):
    region = None
    def __init__(self, config_file):
        cfg_obj = Config(config_file)
        self.ocrConfig = cfg_obj.getConfig("OCR_CONFIG")

    def getWhichOcr(self):
        return self.ocrConfig["USE_AZURE"], self.ocrConfig["USE_VISION"], self.ocrConfig["USE_TEXTRACT"]

    def getChunkSize(self):
        return self.ocrConfig["CHUNK_SIZE"]

    def get_ocr_region(self):
        if GetOCRConfig.region is None:
            GetOCRConfig.region = self.ocrConfig['REGION_NAME']
        
        return GetOCRConfig.region
        
class LoggerConfig(Config):
    def __init__(self, config_file):
        cfg_obj = Config(config_file)
        self.logger_cfg = cfg_obj.getConfig("LOGGER")

    def getLoggerConfig(self):
        return {
            "show_warning": self.logger_cfg["SHOW_WARNING"],
            "show_error": self.logger_cfg["SHOW_ERROR"],
            "show_debug": self.logger_cfg["SHOW_DEBUG"],
            "show_info": self.logger_cfg["SHOW_INFO"]
        }
    
    def showStdprint(self):
        global show_stdprint
        show_stdprint = self.logger_cfg["STDPRINT"]
        print(f"Setted stdprint as: {show_stdprint}")
        return self.logger_cfg["STDPRINT"]

class Database(Config):
    def __init__(self, config_file):
        cfg_obj = Config(config_file)
        self.db = cfg_obj.getConfig("DATABASE")

    def getJobManagerDB(self):
        return self.db["JOB_MANAGER"]

    def getDocumentsDB(self):
        return self.db["DOCUMENTS"]

    def getOcrDB(self):
        return self.db["OCR"]

class S3Operations:
    @staticmethod
    def list_bucket(bucketname, prefix):
        objects = []
        bucket = s3_resource.Bucket(bucketname)

        for bucket_object in bucket.objects.filter(Prefix=prefix):
            objects.append(bucket_object.key)
        
        return objects


class Model(Downloader):
    def __init__(self, config_file):
        # open and load config file
        config_loader = open(config_file, "r")
        config = yaml.load(config_loader, Loader=yaml.FullLoader)
        self.config = config

    def load_model_path(self, key):

        if key == "model":
            model_config = self.config[key]
            model_location = model_config["bucket-info"]

            S3_BUCKET_NAME = model_location["S3_BUCKET_NAME"]
            S3_SUBDIR = model_location["S3_PREFIX"]

            OBJECT_NAME = model_config["MODEL_NAME"]
            S3_PREFIX = []
            OUTPATH = []
            for obj_name in OBJECT_NAME:
                S3_PREFIX.append(os.path.join(S3_SUBDIR, OBJECT_NAME[obj_name]))
                OUTPATH.append(os.path.join(model_config["MODEL_BASE_PATH"], OBJECT_NAME[obj_name]))

            logger.info(f"S3 bucket name: {S3_BUCKET_NAME}")
            logger.info(f"S3 subdirectory: {S3_SUBDIR}")
            logger.info(f"S3 object name/model name: {OBJECT_NAME}")
            logger.info(f"S3 prefix: {S3_PREFIX}")
            logger.info(f"Model saving: {OUTPATH}")

            self.model_location = model_location
            self.S3_BUCKET_NAME = S3_BUCKET_NAME
            self.S3_SUBDIR = S3_SUBDIR
            self.OBJECT_NAME = OBJECT_NAME
            self.S3_PREFIX = S3_PREFIX
            self.S3_SUBDIR = S3_SUBDIR
            self.OUTPATH = OUTPATH

            return True

class Data(Model, Downloader):
    def __init__(self, config_file=None, bucket_name=None, prefix=None, filename=None):

        if config_file != None:
            Model.__init__(self, config_file)

        if bucket_name != None and prefix != None and filename != None:
            Downloader.__init__(self, bucket_name, prefix, filename)

        pass

    def fetchConfigFromS3(self, bucket_name, prefix, filename):
        logger.info("Fetching configuration file..")
        status = Downloader(bucket_name, prefix, filename).getStatus()
        return status

    def getModel(self, config_file):
        logger.info("Fetching model file from s3..")
        model_download_status = Model(config_file).fetchModelFromS3("model")
        logger.info(model_download_status)
        return model_download_status