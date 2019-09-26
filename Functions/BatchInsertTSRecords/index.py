import json
import time
from base64 import b64encode, b64decode
from multiprocessing import Process, Pipe
import dynamo_helper
import logging
import boto3
import itertools
import datetime
from pvapps_odm.Schema.models import TSModelB
from pvapps_odm.session import dynamo_session
sess = dynamo_session(TSModelB)

logging.basicConfig(level=logging.ERROR)

dynamo_put_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource("dynamodb")
serializer = boto3.dynamodb.types.TypeSerializer()

DATETIME_FOMRAT = "%Y-%m-%d %H:%M:%S"


def extract_data_from_kinesis(event):
    logging.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        print(data)
        all_records.append(data)

    logging.info("All records : {}".format((all_records)))

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
            'did_date_measure' : d['deviceId'] + "_" + d['timestamp'].split()[0] + "_" + "speed",
            'tstime' : time.mktime(datetime.datetime.strptime(d['timestamp'], DATETIME_FOMRAT).timetuple()),
            'span_id' : d["spanId"],
            'value' : str(d['speed'])
        }
        data_as_ODM_model.append(TSModelB(**d2))

        d2 = {
            'did_date_measure' : d['deviceId'] + "_" + d['timestamp'].split()[0] + "_" + "latitude",
            'tstime' : time.mktime(datetime.datetime.strptime(d['timestamp'], DATETIME_FOMRAT).timetuple()),
            'span_id' : d["spanId"],
            'value' : str(d['latitude'])
        }
        data_as_ODM_model.append(TSModelB(**d2))

        d2 = {
            'did_date_measure' : d['deviceId'] + "_" + d['timestamp'].split()[0] + "_" + "longitude",
            'tstime' : time.mktime(datetime.datetime.strptime(d['timestamp'], DATETIME_FOMRAT).timetuple()),
            'span_id' : d["spanId"],
            'value' : str(d['longitude'])
        }
        data_as_ODM_model.append(TSModelB(**d2))

        if len(data_as_ODM_model) % 25 == 0:
            print('commiting items')
            sess.add_items(data_as_ODM_model)
            sess.commit_items()
            data_as_ODM_model = []

    sess.add_items(data_as_ODM_model)
    sess.commit_items()


def lambda_handler(event, context):

    data = extract_data_from_kinesis(event)

    put_data_into_TS_dynamo_modelB(data)

    return 'Done adding records to daynamo using ODM'
