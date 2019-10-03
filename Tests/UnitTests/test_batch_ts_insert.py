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
logger.setLevel(logging.INFO)

import os
import mock


class TestBatchTSInsert(unittest.TestCase):
    def test_extract_data_from_kinesis(self):
        with open("sample_ts_insert_input.json") as f:
            event = json.load(f)
        extract_data_from_kinesis(event)
        logger.debug("Event is : \n{}".format(pformat(event)))



def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBatchTSInsert))
    return suite
