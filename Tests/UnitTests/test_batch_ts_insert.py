import json
import datetime
import ciso8601
import time
import logging
import os
import sys
import unittest
from pprint import pformat, pprint

os.environ["TSModelCTable"] = "sahil_test_TSMODELC"
# os.environ["localhost"] = "1"

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from Functions.BatchInsertTSRecords.index import (
    handler,
    extract_data_from_kinesis,
    put_data_into_TS_dynamo_modelC,
)

from pvapps_odm.ddbcon import dynamo_dbcon
from pvapps_odm.Schema.models import TSModelC
from pvapps_odm.ddbcon import Connection


logger = logging.getLogger("test_batch_ts_insert")
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

import os
import mock


class TestBatchTSInsert(unittest.TestCase):

    if "localhost" in os.environ:
        print("Testing on localhost")
        ddb = dynamo_dbcon(TSModelC, Connection(host="http://localhost:8000"))
    else:
        ddb = dynamo_dbcon(TSModelC)
    ddb.connect()

    @classmethod
    def setUpClass(cls):
        TSModelC.create_table()
        if "localhost" in os.environ:
            time.sleep(1)
        else:
            time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        if "localhost" in os.environ:
            time.sleep(1)
            TSModelC.delete_table()

    def test_extract_data_from_kinesis(self):
        with open("sample_ts_insert_input.json") as f:
            event = json.load(f)
        logger.debug("Event is : \n{}".format(pformat(event)))

        extracted_data = extract_data_from_kinesis(event)

        logger.debug(
            "Extracted event is : \n{}".format(pformat(extracted_data))
        )

        expected_extracted_data = [
            {
                "GPSTime": "2019-05-22T10:45:05Z",
                "acc": "0",
                "alt": "41.00",
                "course": "63.60",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8250N",
                "lng": "622.34220W",
                "spanId": "123",
                "speed": "0.28",
                "status": "1",
                "timestamp": "2019-05-22 10:45:05.154000",
            },
            {
                "GPSTime": "2019-05-22T10:45:06Z",
                "acc": "0",
                "alt": "41.00",
                "course": "148.87",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8250N",
                "lng": "622.34210W",
                "spanId": "123",
                "speed": "0.83",
                "status": "1",
                "timestamp": "2019-05-22 10:45:06.154000",
            },
            {
                "GPSTime": "2019-05-22T10:45:07Z",
                "acc": "0",
                "alt": "41.00",
                "course": "137.71",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "geoid": "55.00",
                "io": "00000000",
                "lat": "5319.8249N",
                "lng": "622.34200W",
                "spanId": "123",
                "speed": "0.52",
                "status": "1",
                "timestamp": "2019-05-22 10:45:07.154000",
            },
        ]

        for e1, e2 in zip(extracted_data, expected_extracted_data):
            self.assertEqual(e1, e2)

    def test_put_data_into_TS_dynamo_modelC(self):
        data = [
            {
                "acc": "1",
                "course": "331.47",
                "deviceId": "112",
                "gpsTime": "2019-10-18T13:59:59.000Z",
                "io": "11111111",
                "lat": "5319.84N",
                "lng": "622.338W",
                "spanId": "f565bfdd-38e4-4533-b6a4-32112044c3b7",
                "speed": "1.31492",
                "status": "valid",
                "timestamp": "2019-10-18 13:00:00.004000+00:00",
            },
            {
                "acc": "1",
                "course": "331.47",
                "deviceId": "111",
                "gpsTime": "2019-10-18T13:59:59.000Z",
                "io": "11111111",
                "lat": "5319.84N",
                "lng": "622.338W",
                "spanId": "f565bfdd-38e4-4533-b6a4-32112044c3b7",
                "speed": "1.31492",
                "status": "valid",
                "timestamp": "2019-10-18 13:00:01.004000+00:00",
            },
        ]

        ans = put_data_into_TS_dynamo_modelC(data)
        pprint(ans)

    def test_put_data_into_TS_dynamo_modelC_duplicate_data(self):
        data = [
            {
                "acc": "1",
                "course": "331.47",
                "deviceId": "112",
                "gpsTime": "2019-10-18T13:59:59.000Z",
                "io": "11111111",
                "lat": "5319.84N",
                "lng": "622.338W",
                "spanId": "f565bfdd-38e4-4533-b6a4-32112044c3b7",
                "speed": "1.31492",
                "status": "valid",
                "timestamp": "2019-10-18 13:00:00.004000+00:00",
            },
            {  # duplicate data
                "acc": "1",
                "course": "331.47",
                "deviceId": "112",
                "gpsTime": "2019-10-18T13:59:59.000Z",
                "io": "11111111",
                "lat": "5319.84N",
                "lng": "622.338W",
                "spanId": "f565bfdd-38e4-4533-b6a4-32112044c3b7",
                "speed": "1.31492",
                "status": "valid",
                "timestamp": "2019-10-18 13:00:00.004000+00:00",
            },
        ]

        non_duplicate_models = put_data_into_TS_dynamo_modelC(data)
        pprint(non_duplicate_models)

        expected_non_duplicate_models_attr_values = [
            {
                "device_id": str(data[0]["deviceId"]),
                "span_id": str(data[0]["spanId"]),
                "tstime": ciso8601.parse_datetime(
                    data[0]["timestamp"]
                ).timestamp(),
                "speed": str(data[0]["speed"]),
                "gps_coords": {
                    "lat": str(data[0]["lat"]),
                    "lng": str(data[0]["lng"]),
                    "gps_timestamp": str(data[0]["timestamp"]),
                    "gps_valid": str(data[0]["status"]),
                },
            }
        ]
        expected_non_duplicate_models = [
            TSModelC(**x) for x in expected_non_duplicate_models_attr_values
        ]

        for m1, m2 in zip(non_duplicate_models, expected_non_duplicate_models):
            self.assertEqual(hash(m1), hash(m2))

    def test_handler(self):
        with open("sample_ts_insert_input.json") as f:
            event = json.load(f)
        logging.debug("Event is : \n{}".format(pformat(event)))

        handler(event, None)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBatchTSInsert))
    return suite
