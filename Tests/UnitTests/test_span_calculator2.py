import boto3
import json
import logging
import os
import sys
import unittest
from pprint import pformat

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

import os
from Functions.CreateTimeseriesRecord.index import (
    handler,
    get_spans_for_devices_from_DAX_batch,
)

logger = logging.getLogger("test_span_calculator2")
logger.setLevel(logging.DEBUG)

import os
os.environ['OutputKinesisStreamName'] = 'dan_dax_output'



class TestCreateTimeSeriesRecord(unittest.TestCase):
    def test_handler(self):
        logger.info("Testing handler")
        with open("./sample_span_calculation_input.json") as f:
            event = json.load(f)
        logger.info("Event is : {}".format(event))
        records = handler(event, None)
        logger.info("Extracted records are :\n {}".format(pformat(records)))

        logger.info("Testing handler...Done")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCreateTimeSeriesRecord))
    return suite
