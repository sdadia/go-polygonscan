from base64 import b64decode
import ciso8601
import json
import logging
from pprint import pformat
import os
import time

from pvapps_odm.Schema.models import TSModelC
from pvapps_odm.ddbcon import dynamo_dbcon, Connection
from pynamodb.exceptions import TableError

###########
# logging #
###########
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

############
# resource #
############
TSModelC.Meta.table_name = os.environ["TSModelCTable"]
ddb = dynamo_dbcon(TSModelC, conn=Connection())
ddb.connect()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def extract_data_from_kinesis(event):
    logger.info("Extracting data from Kinesis")

    all_records = []
    for r in event["Records"]:
        data = b64decode(r["kinesis"]["data"]).decode("utf-8")
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        all_records.append(data)

    logger.info("Extracted records are : \n{}".format(pformat(all_records)))

    logger.info("Extracting data from Kinesis...Done")

    return all_records


def put_data_into_TS_dynamo_modelC(data):
    """
    Input is a  list of data that you want to convert as model
    []
    """
    data_as_model = []
    keys_already_added = {}

    # gather the data for modelC
    for d in data:
        try:
            data = {
                "device_id": str(d["deviceId"]),
                "span_id": str(d["spanId"]),
                "tstime": ciso8601.parse_datetime(d["timestamp"]).timestamp(),
                "timezone" : str(d["timezone"]),
                "mileage": str(d["mileage"]),
                "dst" : str(d['offset']),
                "speed": str(d["speed"]),
                "io": str(d["io"]),
                "acc": str(d["acc"]),
                "gps_coords": {
                    "lat": str(d["lat"]),
                    "lng": str(d["lng"]),
                    "gps_timestamp": str(d["gpsTime"]),
                    "gps_valid": str(d["status"]),
                    "course": str(d["course"]),
                },
            }

            data_as_model.append(TSModelC(**data))
        except KeyError as e:
            logger.error(f'ERROR: {e}')
            logger.error(d)

    # de-duplicate the models
    # the model you are using must contain a hash and eq function
    non_duplicate_models = list(set(data_as_model))
    # warn user if duplicate values were removed
    if len(data_as_model) != len(non_duplicate_models):
        logger.error(
            "Found some duplicate values, removing them and moving on!"
        )
        logger.error(
            "The duplicates are based on the spanid and timestamp and NOT on the attribute values"
        )
        logger.error("The data is : {}".format(data))

    retry_number = 1
    try:
        ddb.session.add_items(non_duplicate_models)
        ddb.session.commit_items()
    # if you get a throttling error, thentry for 3 times, with increasing sleep
    # time upto max of 3 seconds
    except TableError as e:
        logger.error("Found Table error : {}".format(e))
        while retry_number <= 3:
            try:
                time.sleep(retry_number)
                ddb.session.commit_items()
                break
            except TableError as e:
                pass
            retry_number += 1
    except Exception as error:
        logger.error("Found unexpected error : {}".format(error))
        raise
    if retry_number == 3:
        logger.error("Retried 3 times. Moving on")

    return non_duplicate_models


def handler(event, context):
    logger.info("Event is : {}".format(event))

    data = extract_data_from_kinesis(event)

    put_data_into_TS_dynamo_modelC(data)

    return "Done adding records to daynamo using ODM"
