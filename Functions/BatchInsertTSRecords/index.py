import json
import time
from base64 import b64encode, b64decode
from multiprocessing import Process, Pipe
import dynamo_helper
from pprint import pprint, pformat
import logging
import boto3
import itertools
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
logger = logging.getLogger("index")

dynamo_put_client = boto3.client("dynamodb")
dynamodb_resource = boto3.resource("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
# DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def extract_data_from_kinesis(event):
    logging.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        all_records.append(data)

    logging.debug("All records : \n{}".format(pformat(all_records)))

    logging.info("Extracting data from Kinesis...Done")

    return all_records


def chunks(l, n=25):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i : i + n]


def put_data_into_TS_dynamo_modelB(data):
    data_as_ODM_model = []
    for d in data:
        d2 = {
            "did_date_measure": d["deviceId"]
            + "_"
            + d["timestamp"].split()[0]
            + "_"
            + "speed",
            "tstime": time.mktime(
                datetime.datetime.strptime(
                    d["timestamp"], DATETIME_FORMAT
                ).timetuple()
            ),
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
            "tstime": time.mktime(
                datetime.datetime.strptime(
                    d["timestamp"], DATETIME_FORMAT
                ).timetuple()
            ),
            "span_id": d["spanId"],
            "value": str(d["latitude"]),
        }
        data_as_ODM_model.append(TSModelB(**d2))

        d2 = {
            "did_date_measure": d["deviceId"]
            + "_"
            + d["timestamp"].split()[0]
            + "_"
            + "longitude",
            "tstime": time.mktime(
                datetime.datetime.strptime(
                    d["timestamp"], DATETIME_FORMAT
                ).timetuple()
            ),
            "span_id": d["spanId"],
            "value": str(d["longitude"]),
        }
        data_as_ODM_model.append(TSModelB(**d2))

    ddb.session.add_items(data_as_ODM_model)
    ddb.session.commit_items()

    #     if len(data_as_ODM_model) % 25 == 0:
    #         print('commiting items')
    #         sess.add_items(data_as_ODM_model)
    #         sess.commit_items()
    #         data_as_ODM_model = []
    # import pprint; pprint.pprint(data_as_ODM_model)

    # # sess.add_items(data_as_ODM_model)
    # # sess.commit_items()
    # ddb.session.add_items(data_as_ODM_model)
    # ddb.session.commit_items()


def handler(event, context):

    data = extract_data_from_kinesis(event)

    put_data_into_TS_dynamo_modelB(data)

    return "Done adding records to daynamo using ODM"
