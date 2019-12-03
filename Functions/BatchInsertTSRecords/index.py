from base64 import b64decode
from pprint import pformat
import ciso8601
import json
import logging
import os

from pvapps_odm.Schema.models import TSModelC
from pvapps_odm.ddbcon import dynamo_dbcon, Connection

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
        print(data)
        data = json.loads(data)
        data = {**data, **data["gps"]}
        del data["gps"]
        all_records.append(data)

    logger.debug("Extracted records are : \n{}".format(pformat(all_records)))

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
        data = {
            "device_id": str(d["deviceId"]),
            "span_id": str(d["spanId"]),
            "tstime": ciso8601.parse_datetime(d["timestamp"]).timestamp(),
            "speed": str(d["speed"]),
            "gps_coords": {
                "lat": str(d["lat"]),
                "lng": str(d["lng"]),
                "gps_timestamp": str(d["timestamp"]),
                "gps_valid": str(d["status"]),
                "course": str(d["course"]),
            },
        }

        data_as_model.append(TSModelC(**data))

    # de-duplicate the models
    # the model you are using must contain a hash and eq function
    non_duplicate_models = list(set(data_as_model))
    # warn user if duplicate values were removed
    if len(data_as_model) != len(non_duplicate_models):
        logger.warning("Found some duplicate values, removing them")

    try:
        ddb.session.add_items(non_duplicate_models)
        ddb.session.commit_items()
    except Exception as e:
        logger.info("Found unexpected error ; {}".format(e))

    return non_duplicate_models


def handler(event, context):
    data = extract_data_from_kinesis(event)

    put_data_into_TS_dynamo_modelC(data)

    return "Done adding records to daynamo using ODM"
