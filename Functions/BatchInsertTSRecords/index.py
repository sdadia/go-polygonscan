import os
import json
import time
from base64 import b64encode, b64decode
from multiprocessing import Process, Pipe
from pprint import pprint, pformat
import logging
import boto3
import itertools
import ciso8601
import datetime
from pvapps_odm.Schema.models import TSModelB

# from pvapps_odm.session import dynamo_session
from pvapps_odm.ddbcon import dynamo_dbcon
from pvapps_odm.Schema.models import SpanModel
from pvapps_odm.ddbcon import Connection

ddb = dynamo_dbcon(TSModelB, conn=Connection())
ddb.connect()

# sess = dynamo_session(TSModelB)


root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

dynamo_put_client = boto3.client("dynamodb")
dynamodb_resource = boto3.resource("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
# DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def extract_data_from_kinesis(event):
    logger.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        all_records.append(data)

    logger.debug("Extracted records are : \n{}".format(pformat(all_records)))

    logger.info("Extracting data from Kinesis...Done")

    return all_records


def chunks(l, n=25):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i : i + n]


def convert_data_to_TSModelB(data, metric_name):

    # convert the data into TS model
    d2 = {
        "did_date_measure": data["deviceId"]
        + "_"
        + data["timestamp"].split()[0]
        + "_"
        + str(metric_name),
        "tstime": datetime.datetime.strptime(
            data["timestamp"], DATETIME_FORMAT
        ).timestamp(),
        "span_id": data["spanId"],
        "value": str(data[metric_name]),
    }

    return TSModelB(**d2)


def put_data_into_TS_dynamo_modelB(data):
    logger.info("Data for conversion : \n{}".format(pformat(data)))

    # convert the data into TS model
    data_as_ODM_model = []
    for d in data:
        d2 = {
            "did_date_measure": d["deviceId"]
            + "_"
            + d["timestamp"].split()[0]
            + "_"
            + "speed",
            "tstime": ciso8601.parse_datetime(d["timestamp"]).timestamp(),
            "span_id": d["spanId"],
            "value": str(d["speed"]),
        }
        data_as_ODM_model.append(TSModelB(**d2))

        d2 = {
            "did_date_measure": d["deviceId"]
            + "_"
            + d["timestamp"].split()[0]
            + "_"
            + "latitude",
            "tstime": ciso8601.parse_datetime(d["timestamp"]).timestamp(),
            "span_id": d["spanId"],
            "value": str(d["lat"]),
        }
        data_as_ODM_model.append(TSModelB(**d2))

        d2 = {
            "did_date_measure": d["deviceId"]
            + "_"
            + d["timestamp"].split()[0]
            + "_"
            + "longitude",
            "tstime": ciso8601.parse_datetime(d["timestamp"]).timestamp(),
            "span_id": d["spanId"],
            "value": str(d["lng"]),
        }
        data_as_ODM_model.append(TSModelB(**d2))

    ddb.session.add_items(data_as_ODM_model)
    ddb.session.commit_items()

    return data_as_ODM_model


def handler(event, context):

    data = extract_data_from_kinesis(event)

    put_data_into_TS_dynamo_modelB(data)

    return "Done adding records to daynamo using ODM"
