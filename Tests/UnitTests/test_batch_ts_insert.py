import json
import datetime
import logging
import os
import sys
import unittest
from pprint import pformat, pprint

from pvapps_odm.Schema.models import TSModelB


logging.getLogger("index").setLevel(logging.INFO)

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from Functions.BatchInsertTSRecords.index import (
    handler,
    extract_data_from_kinesis,
)


logger = logging.getLogger("test_batch_ts_insert")
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

import os
import mock


class TestBatchTSInsert(unittest.TestCase):
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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBatchTSInsert))
    return suite
