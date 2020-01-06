import pytz
import ciso8601
import json
import datetime
import logging
import os
import sys
import unittest
from pprint import pformat, pprint
from unittest import mock

os.environ["localhost"] = "1"
os.environ["OutputKinesisStreamName"] = "pvcam-ProcessedTelematicsStream-test"
os.environ[
    "SpanDynamoDBTableName"
] = "sahil_test_span_table_prefix_from_environment_var"

from pvapps_odm.Schema.models import SpanModel

SpanModel.Meta.table_name = os.environ[
    "SpanDynamoDBTableName"
] + datetime.datetime.utcnow().strftime("%d%m%Y")


logging.getLogger("Functions.CreateTimeseriesRecord.index").setLevel(
    logging.ERROR
)

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from Functions.CreateTimeseriesRecord.index import (
    handler,
    get_all_records_in_event,
    remove_invalid_trip_data,
    get_unique_device_ids_from_records,
    get_spans_for_devices_from_DAX_batch_usingODM,
    process_spans,
    DATETIME_FORMAT,
    update_modified_device_spans_in_dynamo_using_ODM,
    _split_span_across_2_days,
    _map_device_spans_to_date,
)


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

import os
import time
from pvapps_odm.ddbcon import dynamo_dbcon
from pynamodb.connection import Connection


class TestCreateTimeSeriesRecord(unittest.TestCase):
    print(SpanModel.Meta.table_name)
    ddb = dynamo_dbcon(SpanModel, Connection(host="http://localhost:8000"))
    ddb.connect()

    @classmethod
    def setUpClass(cls):

        SpanModel.create_table()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        time.sleep(1)
        SpanModel.delete_table()

    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_spans_for_devices_from_DAX_batch_usingODM"
    )
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_all_records_in_event"
    )
    def test_handler_mock(
        self,
        mock_get_all_records_in_event,
        mock_get_spans_for_devices_from_DAX_batch_usingODM,
    ):
        mock_get_all_records_in_event.return_value = [
            [
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:05Z",
                        "alt": "41.00",
                        "course": "63.60",
                        "geoid": "55.00",
                        "lat": "5319.8250N",
                        "lng": "622.34220W",
                        "speed": "0.28",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:05.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:06Z",
                        "alt": "41.00",
                        "course": "148.87",
                        "geoid": "55.00",
                        "lat": "5319.8250N",
                        "lng": "622.34210W",
                        "speed": "0.83",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:47:06.154Z",
                },
            ]
        ]

        mock_get_spans_for_devices_from_DAX_batch_usingODM.return_value = [
            {
                "spans": [
                    {
                        "start_time": pytz.UTC.localize(
                            datetime.datetime(2019, 5, 22, 10, 45, 5, 154000)
                        ),
                        "end_time": pytz.UTC.localize(
                            datetime.datetime(2019, 5, 22, 10, 45, 14, 154000)
                        ),
                        "spanId": "123",
                    }
                ],
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
            }
        ]
        logger.info("Testing handler")
        handler({}, None)

        logger.info("Testing handler...Done")

    def test_get_all_records_in_event(self):
        logging.info("Testing get_all_records_in_event function")
        with open("sample_span_calculation_input.json") as f:
            event = json.load(f)
        logger.debug("Event is : {}".format(event))
        all_records = get_all_records_in_event(event)

        logger.debug(
            "Extracted records are : \n{}".format(pformat(all_records))
        )

        expected_all_records = [
            [
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:05Z",
                        "alt": "41.00",
                        "course": "63.60",
                        "geoid": "55.00",
                        "lat": "5319.8250N",
                        "lng": "622.34220W",
                        "speed": "0.28",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:05.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:06Z",
                        "alt": "41.00",
                        "course": "148.87",
                        "geoid": "55.00",
                        "lat": "5319.8250N",
                        "lng": "622.34210W",
                        "speed": "0.83",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:06.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:07Z",
                        "alt": "41.00",
                        "course": "137.71",
                        "geoid": "55.00",
                        "lat": "5319.8249N",
                        "lng": "622.34200W",
                        "speed": "0.52",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:07.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:08Z",
                        "alt": "41.10",
                        "course": "289.18",
                        "geoid": "55.00",
                        "lat": "5319.8249N",
                        "lng": "622.34190W",
                        "speed": "0.69",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:08.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:09Z",
                        "alt": "41.10",
                        "course": "250.05",
                        "geoid": "55.00",
                        "lat": "5319.8249N",
                        "lng": "622.34210W",
                        "speed": "0.76",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:09.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:10Z",
                        "alt": "41.10",
                        "course": "288.74",
                        "geoid": "55.00",
                        "lat": "5319.8249N",
                        "lng": "622.34230W",
                        "speed": "1.06",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:10.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:11Z",
                        "alt": "41.10",
                        "course": "324.73",
                        "geoid": "55.00",
                        "lat": "5319.8249N",
                        "lng": "622.34250W",
                        "speed": "1.13",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:11.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:12Z",
                        "alt": "41.10",
                        "course": "335.31",
                        "geoid": "55.00",
                        "lat": "5319.8248N",
                        "lng": "622.34210W",
                        "speed": "1.37",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:12.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:13Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "5319.8246N",
                        "lng": "622.34160W",
                        "speed": "0.98",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:13.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:14Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "5319.8246N",
                        "lng": "622.34160W",
                        "speed": "0.98",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:14.154Z",
                },
            ]
        ]
        print(all_records)

        self.assertEqual(len(all_records), len(expected_all_records))
        for e1, e2 in zip(expected_all_records[0], all_records[0]):
            self.assertEqual(e1, e2)
        logging.info("Testing get_all_records_in_event function...Done")

    def test_remove_invalid_trip_data(self):
        """
        Test if we can remvoe invalid records from a list of records
        """
        logging.info("Testing remove_invalid_trip_data")
        data = [
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "5319.8246N",
                    "lng": "622.34160W",
                    "speed": "0.98",
                    "status": "1",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "0.0",
                    "lng": "0.0",
                    "speed": "0.0",
                    "status": "0",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
        ]
        valid_data = remove_invalid_trip_data(data)
        self.assertEqual(len(valid_data), 1)
        self.assertEqual(valid_data[0], data[0])
        logging.info("Testing remove_invalid_trip_data...Done")

    def test_remove_invalid_trip_data_all_invalid(self):
        """
        Test if we can remvoe invalid records from a list of records
        """
        logging.info("Testing remove_invalid_trip_data")
        data = [
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "5319.8246N",
                    "lng": "622.34160W",
                    "speed": "0.98",
                    "status": "0",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "10.0",
                    "lng": "10.0",
                    "speed": "10.0",
                    "status": "0",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
        ]
        valid_data = remove_invalid_trip_data(data)
        self.assertEqual(len(valid_data), 0)
        logging.info("Testing remove_invalid_trip_data...Done")

    def test_remove_invalid_trip_data_all_valid(self):
        """
        Test if we can remvoe invalid records from a list of records
        """
        logging.info("Testing remove_invalid_trip_data")
        data = [
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "5319.8246N",
                    "lng": "622.34160W",
                    "speed": "0.98",
                    "status": "1",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
            {
                "acc": "0",
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
                "gps": {
                    "GPSTime": "2019-05-22T10:45:14Z",
                    "alt": "41.10",
                    "course": "326.74",
                    "geoid": "55.00",
                    "lat": "10.0",
                    "lng": "10.0",
                    "speed": "10.0",
                    "status": "1",
                },
                "io": "00000000",
                "timeStamp": "2019-05-22T10:45:14.154Z",
            },
        ]
        valid_data = remove_invalid_trip_data(data)
        self.assertEqual(len(valid_data), 2)
        for e1, e2 in zip(data, valid_data):
            self.assertEqual(e1, e2)
        logging.info("Testing remove_invalid_trip_data...Done")

    def test_remove_invalid_trip_data_no_data(self):
        """
        Test if we can remvoe invalid records from a list of records
        """
        logging.info("Testing remove_invalid_trip_data")
        data = []
        valid_data = remove_invalid_trip_data(data)
        self.assertEqual(len(valid_data), 0)
        logging.info("Testing remove_invalid_trip_data...Done")

    # @unittest.SkipTest
    def test_handler2(self):
        with open("sample_span_calculation_input.json") as f:
            event = json.load(f)
        handler(event, None)

    def test_get_unique_device_ids_from_records(self):
        logging.info("Testing get_unique_device_ids_from_records")
        all_records = [
            [
                {
                    "acc": "0",
                    "deviceId": "1",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:14Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "5319.8246N",
                        "lng": "622.34160W",
                        "speed": "0.98",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:14.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "2",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:14Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "10.0",
                        "lng": "10.0",
                        "speed": "10.0",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:14.154Z",
                },
            ]
        ]

        ans = get_unique_device_ids_from_records(all_records)
        self.assertEqual(type(ans), list)
        for r in ans:
            self.assertTrue(r["deviceId"] in ["1", "2"])
        logging.info("Testing get_unique_device_ids_from_records...Done")

    def test_get_unique_device_ids_from_records_single_unique(self):
        logging.info("Testing get_unique_device_ids_from_records")
        all_records = [
            [
                {
                    "acc": "0",
                    "deviceId": "1",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:14Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "5319.8246N",
                        "lng": "622.34160W",
                        "speed": "0.98",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:14.154Z",
                },
                {
                    "acc": "0",
                    "deviceId": "1",
                    "gps": {
                        "GPSTime": "2019-05-22T10:45:14Z",
                        "alt": "41.10",
                        "course": "326.74",
                        "geoid": "55.00",
                        "lat": "10.0",
                        "lng": "10.0",
                        "speed": "10.0",
                        "status": "1",
                    },
                    "io": "00000000",
                    "timeStamp": "2019-05-22T10:45:14.154Z",
                },
            ]
        ]

        ans = get_unique_device_ids_from_records(all_records)
        self.assertEqual(type(ans), list)
        self.assertEqual(len(ans), 1)
        for r in ans:
            self.assertTrue(r["deviceId"] in ["1"])
        logging.info("Testing get_unique_device_ids_from_records...Done")

    @mock.patch("Functions.CreateTimeseriesRecord.index.ddb")
    def test_get_spans_for_devices_from_DAX_batch_usingODM_no_spans_exist(
        self, mock_dbcon
    ):
        mock_dbcon.batch_get.return_value = []
        ans = get_spans_for_devices_from_DAX_batch_usingODM(
            [{"deviceId": "123"}]
        )
        self.assertEqual(len(ans), 0)

    @mock.patch("Functions.CreateTimeseriesRecord.index.ddb")
    def test_get_spans_for_devices_from_DAX_batch_usingODM_span_exist(
        self, mock_dbcon
    ):
        data_for_spans = {
            "deviceId": "123",
            "spans": json.dumps(
                [
                    {
                        "spanId": "1",
                        "start_time": "2019-05-22T10:45:05.154000Z",
                        "end_time": "2019-05-22T10:47:06.154000Z",
                    }
                ]
            ),
        }
        mock_dbcon.batch_get.return_value = [SpanModel(**data_for_spans)]
        ans = get_spans_for_devices_from_DAX_batch_usingODM(
            [{"deviceId": "123"}]
        )
        self.assertEqual(len(ans), 1)

    # class TestProcessSpans(unittest.TestCase):
    current_device_spans = {
        "deviceId": "123",
        "spans": [
            {
                "spanId": "1",
                "start_time": datetime.datetime.strptime(
                    "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                ),
                "end_time": datetime.datetime.strptime(
                    "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                ),
            },
            {
                "spanId": "2",
                "start_time": datetime.datetime.strptime(
                    "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                ),
                "end_time": datetime.datetime.strptime(
                    "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                ),
            },
        ],
    }

    # @unittest.SkipTest
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_all_records_in_event"
    )
    def test_multiple_device_per_invocation(self, mock_get_all_data):
        """
        This test is made to check if we can do multiple devices per invoation
        """
        messages = [
            {
                "message": {
                    "timeStamp": "2019-11-01T01:32:21.017Z",
                    "id": "c8af461d-2ffb-4b2d-a1e8-212148bb56dd",
                    "from": "1da54fb7-b6c3-439b-ba59-de6f8a972ab0",
                    "to": "ab9ynjbbyn7jl-ats.iot.us-east-1.amazonaws.com",
                    "type": "event",
                    "correlation": "",
                    "expiration": "",
                    "compressed": "true",
                    "payload": {
                        "type": "deviceStatus",
                        "context": {
                            "deviceTime": "2019-11-01T01:32:21.018Z",
                            "tracking": [
                                {
                                    "timeStamp": "2019-11-01T01:32:11.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:11.000Z",
                                        "lat": "5319.864100N",
                                        "lng": "622.372860W",
                                        "speed": "000.78",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:12.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:12.000Z",
                                        "lat": "5319.864110N",
                                        "lng": "622.373420W",
                                        "speed": "000.56",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:13.007Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:13.000Z",
                                        "lat": "5319.864150N",
                                        "lng": "622.373920W",
                                        "speed": "000.43",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:14.006Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:14.000Z",
                                        "lat": "5319.864130N",
                                        "lng": "622.374300W",
                                        "speed": "000.46",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:15.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:15.000Z",
                                        "lat": "5319.864180N",
                                        "lng": "622.374770W",
                                        "speed": "000.33",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:16.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:16.000Z",
                                        "lat": "5319.863940N",
                                        "lng": "622.374720W",
                                        "speed": "000.62",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:17.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:17.000Z",
                                        "lat": "5319.863770N",
                                        "lng": "622.374630W",
                                        "speed": "000.65",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:18.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:18.000Z",
                                        "lat": "5319.863680N",
                                        "lng": "622.374600W",
                                        "speed": "000.49",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:19.012Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:19.000Z",
                                        "lat": "5319.863710N",
                                        "lng": "622.374660W",
                                        "speed": "000.27",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:20.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:20.000Z",
                                        "lat": "5319.863590N",
                                        "lng": "622.374400W",
                                        "speed": "000.66",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T01:32:21.011Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T01:32:21.000Z",
                                        "lat": "5319.863600N",
                                        "lng": "622.374410W",
                                        "speed": "000.36",
                                        "course": "00.000",
                                    },
                                },
                            ],
                        },
                    },
                },
                "vehicleId": "b9da1acd-c77f-4b6a-a942-e8d4b30ae65e",
                "orgId": "52a7f45f-db9b-4c6d-ab76-0f12a502598b",
                "nodeId": "cdf83953-557c-4d42-b732-3e5a8bd147c9",
                "deviceId": "83a903e0-2ab5-478b-a3c8-c657626f5c37",
            },
            {
                "message": {
                    "timeStamp": "2019-11-01T08:13:31.006Z",
                    "id": "0eafdc31-b3b5-401f-a617-82fcb289c912",
                    "from": "0551fd10-326a-4207-9c68-79e0bba85e0a",
                    "to": "ab9ynjbbyn7jl-ats.iot.us-east-1.amazonaws.com",
                    "type": "event",
                    "correlation": "",
                    "expiration": "",
                    "compressed": "true",
                    "payload": {
                        "type": "deviceStatus",
                        "context": {
                            "deviceTime": "2019-11-01T08:13:31.007Z",
                            "tracking": [
                                {
                                    "timeStamp": "2019-11-01T08:13:21.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:21.000Z",
                                        "lat": "5334.995600N",
                                        "lng": "607.864300W",
                                        "speed": "000.30",
                                        "course": "332.770",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:22.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:22.000Z",
                                        "lat": "5334.995600N",
                                        "lng": "607.864300W",
                                        "speed": "000.15",
                                        "course": "270.740",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:23.008Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:23.000Z",
                                        "lat": "5334.995700N",
                                        "lng": "607.864400W",
                                        "speed": "000.20",
                                        "course": "300.410",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:24.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:24.000Z",
                                        "lat": "5334.995700N",
                                        "lng": "607.864400W",
                                        "speed": "000.07",
                                        "course": "304.250",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:25.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:25.000Z",
                                        "lat": "5334.995700N",
                                        "lng": "607.864400W",
                                        "speed": "000.11",
                                        "course": "245.650",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:14:24.945Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:26.000Z",
                                        "lat": "5334.995700N",
                                        "lng": "607.864500W",
                                        "speed": "000.44",
                                        "course": "329.880",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:27.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:27.000Z",
                                        "lat": "5334.995800N",
                                        "lng": "607.864500W",
                                        "speed": "000.15",
                                        "course": "250.390",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:28.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:28.000Z",
                                        "lat": "5334.995800N",
                                        "lng": "607.864600W",
                                        "speed": "000.26",
                                        "course": "258.630",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:29.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:29.000Z",
                                        "lat": "5334.995800N",
                                        "lng": "607.864600W",
                                        "speed": "000.31",
                                        "course": "336.340",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:30.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:30.000Z",
                                        "lat": "5334.995800N",
                                        "lng": "607.864700W",
                                        "speed": "000.39",
                                        "course": "338.260",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:31.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:31.000Z",
                                        "lat": "5334.995900N",
                                        "lng": "607.864700W",
                                        "speed": "000.31",
                                        "course": "349.720",
                                    },
                                },
                            ],
                            "faults": [
                                {
                                    "type": "storage",
                                    "status": "missing",
                                    "context": {
                                        "medium": "SD",
                                        "storageClass": "secondary",
                                        "id": "2",
                                    },
                                }
                            ],
                        },
                    },
                },
                "vehicleId": "b9da1acd-c77f-4b6a-a942-e8d4b30ae65e",
                "orgId": "52a7f45f-db9b-4c6d-ab76-0f12a502598b",
                "nodeId": "cdf83953-557c-4d42-b732-3e5a8bd147c9",
                "deviceId": "1968306a-add5-42e6-958f-423caac6d4e1",
            },
            {
                "message": {
                    "timeStamp": "2019-11-01T08:13:20.006Z",
                    "id": "0eafdc31-b3b5-401f-a617-82fcb289c912",
                    "from": "0551fd10-326a-4207-9c68-79e0bba85e0a",
                    "to": "ab9ynjbbyn7jl-ats.iot.us-east-1.amazonaws.com",
                    "type": "event",
                    "correlation": "",
                    "expiration": "",
                    "compressed": "true",
                    "payload": {
                        "type": "deviceStatus",
                        "context": {
                            "deviceTime": "2019-11-01T08:13:20.007Z",
                            "tracking": [
                                {
                                    "timeStamp": "2019-11-01T08:13:10.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:10.000Z",
                                        "lat": "5334.994800N",
                                        "lng": "607.864400W",
                                        "speed": "000.19",
                                        "course": "00.940",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:11.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:11.000Z",
                                        "lat": "5334.994700N",
                                        "lng": "607.864200W",
                                        "speed": "000.28",
                                        "course": "327.100",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:12.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:12.000Z",
                                        "lat": "5334.994800N",
                                        "lng": "607.863900W",
                                        "speed": "000.57",
                                        "course": "18.810",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:13.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:13.000Z",
                                        "lat": "5334.994800N",
                                        "lng": "607.864000W",
                                        "speed": "000.69",
                                        "course": "350.840",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:14.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:14.000Z",
                                        "lat": "5334.994900N",
                                        "lng": "607.864000W",
                                        "speed": "000.89",
                                        "course": "348.340",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:15.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:15.000Z",
                                        "lat": "5334.995000N",
                                        "lng": "607.864100W",
                                        "speed": "000.78",
                                        "course": "00.920",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:16.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:16.000Z",
                                        "lat": "5334.995100N",
                                        "lng": "607.864100W",
                                        "speed": "000.94",
                                        "course": "356.770",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:17.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:17.000Z",
                                        "lat": "5334.995300N",
                                        "lng": "607.864100W",
                                        "speed": "001.02",
                                        "course": "343.340",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:18.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:18.000Z",
                                        "lat": "5334.995400N",
                                        "lng": "607.864200W",
                                        "speed": "000.80",
                                        "course": "350.930",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:19.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:19.000Z",
                                        "lat": "5334.995600N",
                                        "lng": "607.864200W",
                                        "speed": "000.31",
                                        "course": "307.750",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:20.003Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:20.000Z",
                                        "lat": "5334.995600N",
                                        "lng": "607.864300W",
                                        "speed": "000.07",
                                        "course": "193.970",
                                    },
                                },
                            ],
                            "faults": [
                                {
                                    "type": "storage",
                                    "status": "missing",
                                    "context": {
                                        "medium": "SD",
                                        "storageClass": "secondary",
                                        "id": "2",
                                    },
                                }
                            ],
                        },
                    },
                },
                "vehicleId": "b9da1acd-c77f-4b6a-a942-e8d4b30ae65e",
                "orgId": "52a7f45f-db9b-4c6d-ab76-0f12a502598b",
                "nodeId": "cdf83953-557c-4d42-b732-3e5a8bd147c9",
                "deviceId": "1968306a-add5-42e6-958f-423caac6d4e1",
            },
            {
                "message": {
                    "timeStamp": "2019-11-01T08:14:02.013Z",
                    "id": "c8af461d-2ffb-4b2d-a1e8-212148bb56dd",
                    "from": "1da54fb7-b6c3-439b-ba59-de6f8a972ab0",
                    "to": "ab9ynjbbyn7jl-ats.iot.us-east-1.amazonaws.com",
                    "type": "event",
                    "correlation": "",
                    "expiration": "",
                    "compressed": "true",
                    "payload": {
                        "type": "deviceStatus",
                        "context": {
                            "deviceTime": "2019-11-01T08:14:02.014Z",
                            "tracking": [
                                {
                                    "timeStamp": "2019-11-01T08:13:52.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:52.000Z",
                                        "lat": "5319.859140N",
                                        "lng": "622.365780W",
                                        "speed": "001.18",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:53.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:53.000Z",
                                        "lat": "5319.858540N",
                                        "lng": "622.365780W",
                                        "speed": "001.12",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:54.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:54.000Z",
                                        "lat": "5319.857950N",
                                        "lng": "622.365780W",
                                        "speed": "001.19",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:55.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:55.000Z",
                                        "lat": "5319.857480N",
                                        "lng": "622.365780W",
                                        "speed": "000.90",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:56.007Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:56.000Z",
                                        "lat": "5319.857060N",
                                        "lng": "622.365770W",
                                        "speed": "000.62",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:57.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:57.000Z",
                                        "lat": "5319.856800N",
                                        "lng": "622.365750W",
                                        "speed": "000.38",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:58.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:58.000Z",
                                        "lat": "5319.856530N",
                                        "lng": "622.365760W",
                                        "speed": "000.63",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:13:59.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:13:59.000Z",
                                        "lat": "5319.856160N",
                                        "lng": "622.365760W",
                                        "speed": "000.80",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:14:00.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:14:00.000Z",
                                        "lat": "5319.855950N",
                                        "lng": "622.365750W",
                                        "speed": "000.56",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:14:01.004Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:14:01.000Z",
                                        "lat": "5319.855790N",
                                        "lng": "622.365760W",
                                        "speed": "000.32",
                                        "course": "00.000",
                                    },
                                },
                                {
                                    "timeStamp": "2019-11-01T08:14:02.005Z",
                                    "acc": "1",
                                    "io": "11111111",
                                    "gps": {
                                        "status": "valid",
                                        "gpsTime": "2019-11-01T08:14:02.000Z",
                                        "lat": "5319.855630N",
                                        "lng": "622.365800W",
                                        "speed": "000.17",
                                        "course": "00.000",
                                    },
                                },
                            ],
                        },
                    },
                },
                "vehicleId": "b9da1acd-c77f-4b6a-a942-e8d4b30ae65e",
                "orgId": "52a7f45f-db9b-4c6d-ab76-0f12a502598b",
                "nodeId": "cdf83953-557c-4d42-b732-3e5a8bd147c9",
                "deviceId": "83a903e0-2ab5-478b-a3c8-c657626f5c37",
            },
        ]

        all_records = []
        for decoded_rec in messages:

            for d in decoded_rec["message"]["payload"]["context"]["tracking"]:
                d["deviceId"] = decoded_rec["message"]["from"]

            data = {
                "deviceId": decoded_rec["deviceId"],
                "data": decoded_rec["message"]["payload"]["context"][
                    "tracking"
                ],
            }

            all_records.append(data["data"])
        mock_get_all_data.return_value = all_records

        handler(None, None)

        # ddb = dynamo_dbcon(SpanModel, Connection(host="http://localhost:8000"))
        # ddb.connect()

        ans = self.ddb.get_object("0551fd10-326a-4207-9c68-79e0bba85e0a", None)
        ans = ans.attribute_values
        pprint(ans)
        expected_1 = {
            "deviceId": "0551fd10-326a-4207-9c68-79e0bba85e0a",
            "spans": [
                {
                    "start_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        8,
                        13,
                        10,
                        3000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "end_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        8,
                        13,
                        31,
                        3000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "spanId": "3d82d2e9-f42f-4889-8142-3d5e719d4b2d",
                }
            ],
            "modified": 1,
        }
        self.assertEqual(ans["deviceId"], expected_1["deviceId"])
        for sp1, sp2 in zip(json.loads(ans["spans"]), (expected_1["spans"])):
            self.assertEqual(sorted(sp1), sorted(sp2))

        ans = self.ddb.get_object("1da54fb7-b6c3-439b-ba59-de6f8a972ab0", None)
        ans = ans.attribute_values
        pprint(ans)
        expected_2 = {
            "deviceId": "1da54fb7-b6c3-439b-ba59-de6f8a972ab0",
            "spans": [
                {
                    "start_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        1,
                        32,
                        11,
                        4000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "end_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        1,
                        32,
                        21,
                        11000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "spanId": "32bb8e03-453d-40ff-bb3c-7bd393afe89b",
                },
                {
                    "start_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        8,
                        13,
                        52,
                        5000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "end_time": datetime.datetime(
                        2019,
                        11,
                        1,
                        8,
                        14,
                        2,
                        5000,
                        tzinfo=datetime.timezone.utc,
                    ),
                    "spanId": "b07ffca5-d574-4250-bd97-9da1fd400241",
                },
            ],
            "modified": 1,
        }
        self.assertEqual(ans["deviceId"], expected_2["deviceId"])
        for sp1, sp2 in zip(json.loads(ans["spans"]), (expected_2["spans"])):
            self.assertEqual(sorted(sp1), sorted(sp2))

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test__split_span_across_2_days(self, mock_generate_uuid):
        mock_generate_uuid.return_value = "123_mock_id_for_next_day"
        data = {
            "start_time": ciso8601.parse_datetime("2019-06-26T23:58:50.006Z"),
            "end_time": ciso8601.parse_datetime("2019-06-27T00:00:10Z"),
            "spanId": "test_span_id",
        }
        expected_day_1_span = {
            "start_time": ciso8601.parse_datetime("2019-06-26T23:58:50.006Z"),
            "end_time": ciso8601.parse_datetime("2019-06-26T23:59:59Z"),
            "spanId": "test_span_id",
        }
        expected_day_2_span = {
            "start_time": ciso8601.parse_datetime("2019-06-27T00:00:00.000Z"),
            "end_time": ciso8601.parse_datetime("2019-06-27T00:00:10Z"),
            "spanId": mock_generate_uuid.return_value,
            "parent": expected_day_1_span["spanId"],
        }
        day_1_span, day_2_span = _split_span_across_2_days(data)
        for e1, e2 in zip(
            [day_1_span, day_2_span], [expected_day_1_span, expected_day_2_span]
        ):
            self.assertEqual(e1["spanId"], e2["spanId"])
            self.assertEqual(e1["start_time"], e2["start_time"])
            self.assertEqual(e1["end_time"], e2["end_time"])

    def test__split_span_across_2_days_raise_assert_error(self):
        # raises an error coz the start/end time are not across 2 days
        data = {
            "start_time": ciso8601.parse_datetime("2019-06-26T23:58:50.006Z"),
            "end_time": ciso8601.parse_datetime("2019-06-26T00:00:10Z"),
            "spanId": "test_span_id",
        }

        self.assertRaises(AssertionError, _split_span_across_2_days, data)

    def test__map_device_spans_to_date(self):
        test_data = [
            {
                "start_time": ciso8601.parse_datetime("2019-06-26T23:58:50Z"),
                "end_time": ciso8601.parse_datetime("2019-06-26T00:00:10Z"),
                "spanId": "span_1_1",
            },
            {
                "start_time": ciso8601.parse_datetime("2019-06-27T13:58:50Z"),
                "end_time": ciso8601.parse_datetime("2019-06-27T14:00:10Z"),
                "spanId": "span_1_2",
            },
            {
                "start_time": ciso8601.parse_datetime("2019-06-27T17:58:50Z"),
                "end_time": ciso8601.parse_datetime("2019-06-27T18:00:10Z"),
                "spanId": "span_1_3",
            },
        ]
        expected_answer = {
            "2019-06-26": [
                {
                    "start_time": ciso8601.parse_datetime(
                        "2019-06-26T23:58:50Z"
                    ),
                    "end_time": ciso8601.parse_datetime("2019-06-26T00:00:10Z"),
                    "spanId": "span_1_1",
                }
            ],
            "2019-06-27": [
                {
                    "start_time": ciso8601.parse_datetime(
                        "2019-06-27T13:58:50Z"
                    ),
                    "end_time": ciso8601.parse_datetime("2019-06-27T14:00:10Z"),
                    "spanId": "span_1_2",
                },
                {
                    "start_time": ciso8601.parse_datetime(
                        "2019-06-27T17:58:50Z"
                    ),
                    "end_time": ciso8601.parse_datetime("2019-06-27T18:00:10Z"),
                    "spanId": "span_1_3",
                },
            ],
        }
        ans = _map_device_spans_to_date(test_data)

        for e1, e2 in zip(ans.values(), expected_answer.values()):
            for m1, m2 in zip(e1, e2):
                self.assertEqual(m1, m2)

        pprint(ans)

    @unittest.SkipTest
    def test_update_modified_device_spans_in_dynamo_using_ODM(self):
        test_data = [
            {
                "deviceId": "test_device_1",
                "spans": [
                    {  # spans acroos 2 days
                        "start_time": ciso8601.parse_datetime(
                            "2019-06-26T23:58:50Z"
                        ),
                        "end_time": ciso8601.parse_datetime(
                            "2019-06-27T00:00:10Z"
                        ),
                        "spanId": "span_1_1",
                    },
                    {
                        "start_time": ciso8601.parse_datetime(
                            "2019-06-26T13:58:50Z"
                        ),
                        "end_time": ciso8601.parse_datetime(
                            "2019-06-26T14:00:10Z"
                        ),
                        "spanId": "span_1_2",
                    },
                ],
                "modified": 1,
            },
            {
                "deviceId": "test_device_3",
                "spans": [
                    {  # spans acroos 2 days
                        "start_time": ciso8601.parse_datetime(
                            "2019-06-26T23:58:50Z"
                        ),
                        "end_time": ciso8601.parse_datetime(
                            "2019-06-27T00:00:10Z"
                        ),
                        "spanId": "span_3_1",
                    },
                    {
                        "start_time": ciso8601.parse_datetime(
                            "2019-06-26T13:58:50Z"
                        ),
                        "end_time": ciso8601.parse_datetime(
                            "2019-06-26T14:00:10Z"
                        ),
                        "spanId": "span_3_2",
                    },
                ],
                "modified": 1,
            },
            {  # span not modified
                "deviceId": "test_device_2",
                "spans": [
                    {
                        "start_time": ciso8601.parse_datetime(
                            "2019-06-26T23:58:50Z"
                        ),
                        "end_time": ciso8601.parse_datetime(
                            "2019-06-27T00:00:10Z"
                        ),
                        "spanId": "span_2_1",
                    }
                ],
            },
        ]
        expected_answer = {
            "2019-06-26": {
                "test_device_1": [
                    {
                        "end_time": datetime.datetime(
                            2019, 6, 26, 14, 0, 10, tzinfo=datetime.timezone.utc
                        ),
                        "spanId": "span_1_2",
                        "start_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            13,
                            58,
                            50,
                            tzinfo=datetime.timezone.utc,
                        ),
                    },
                    {
                        "end_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            23,
                            59,
                            59,
                            tzinfo=datetime.timezone.utc,
                        ),
                        "spanId": "span_1_1",
                        "start_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            23,
                            58,
                            50,
                            tzinfo=datetime.timezone.utc,
                        ),
                    },
                ],
                "test_device_3": [
                    {
                        "end_time": datetime.datetime(
                            2019, 6, 26, 14, 0, 10, tzinfo=datetime.timezone.utc
                        ),
                        "spanId": "span_3_2",
                        "start_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            13,
                            58,
                            50,
                            tzinfo=datetime.timezone.utc,
                        ),
                    },
                    {
                        "end_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            23,
                            59,
                            59,
                            tzinfo=datetime.timezone.utc,
                        ),
                        "spanId": "span_3_1",
                        "start_time": datetime.datetime(
                            2019,
                            6,
                            26,
                            23,
                            58,
                            50,
                            tzinfo=datetime.timezone.utc,
                        ),
                    },
                ],
            },
            "2019-06-27": {
                "test_device_1": [
                    {
                        "end_time": datetime.datetime(
                            2019, 6, 27, 0, 0, 10, tzinfo=datetime.timezone.utc
                        ),
                        "spanId": "span_1_1_splitted",
                        "start_time": datetime.datetime(
                            2019, 6, 27, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                    }
                ],
                "test_device_3": [
                    {
                        "end_time": datetime.datetime(
                            2019, 6, 27, 0, 0, 10, tzinfo=datetime.timezone.utc
                        ),
                        "spanId": "span_3_1_splitted",
                        "start_time": datetime.datetime(
                            2019, 6, 27, 0, 0, tzinfo=datetime.timezone.utc
                        ),
                    }
                ],
            },
        }
        update_modified_device_spans_in_dynamo_using_ODM(test_data)


class TestProcessSpans(unittest.TestCase):
    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_first_time_entry_start_lat_not_exist(
        self, mock_generate_uuid
    ):
        mock_generate_uuid.return_value = "1abc"
        current_device_spans = []
        array_start_time = "2019-05-22T10:45:05.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:45:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans,
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            None,
            None,
            "end_lat",
            "end_lng",
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        # compare the start and end lat
        self.assertEqual(all_spans[0]["start_lat"], "end_lat")
        self.assertEqual(all_spans[0]["start_lng"], "end_lng")
        self.assertEqual(all_spans[0]["end_lat"], "end_lat")
        self.assertEqual(all_spans[0]["end_lng"], "end_lng")

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_first_time_entry_end_lat_not_exist(
        self, mock_generate_uuid
    ):
        mock_generate_uuid.return_value = "1abc"
        current_device_spans = []
        array_start_time = "2019-05-22T10:45:05.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:45:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans,
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            None,
            None,
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        # compare the start and end lat
        self.assertEqual(all_spans[0]["start_lat"], "start_lat")
        self.assertEqual(all_spans[0]["start_lng"], "start_lng")
        self.assertEqual(all_spans[0]["end_lat"], "start_lat")
        self.assertEqual(all_spans[0]["end_lng"], "start_lng")

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_first_time_entry(self, mock_generate_uuid):
        mock_generate_uuid.return_value = "1abc"
        current_device_spans = []
        array_start_time = "2019-05-22T10:45:05.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:45:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans,
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        # compare the start and end lat
        self.assertEqual(all_spans[0]["start_lat"], "start_lat")
        self.assertEqual(all_spans[0]["start_lng"], "start_lng")
        self.assertEqual(all_spans[0]["end_lat"], "end_lat")
        self.assertEqual(all_spans[0]["end_lng"], "end_lng")

    def test_process_spans_inside(self):
        """
        Span     x----------------------x
        Data              x----x
        """

        # current_device_spans = self.current_device_spans

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
            ],
        }

        array_start_time = "2019-05-22T10:45:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:45:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, False)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(
            all_spans[0]["start_time"],
            current_device_spans["spans"][0]["start_time"],
        )
        self.assertEqual(
            all_spans[0]["end_time"],
            current_device_spans["spans"][0]["end_time"],
        )

        self.assertEqual(all_spans[0]["start_lat"], "1")
        self.assertEqual(all_spans[0]["start_lng"], "2")
        self.assertEqual(all_spans[0]["end_lat"], "3")
        self.assertEqual(all_spans[0]["end_lng"], "4")

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_right_new_span_start_lat_lng_does_not_exist(
        self, mock_generate_uuid
    ):
        """
        Span     x----------------------x
        Data                                  x----x
        """

        mock_generate_uuid.return_value = "1abc"
        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                # new span will be added here
            ],
        }

        array_start_time = "2019-05-22T13:13:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T13:25:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            None,
            None,
            "end_lat",
            "end_lng",
        )
        # print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 3)
        self.assertEqual(spanId_for_tagging, mock_generate_uuid.return_value)
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)
        self.assertEqual(all_spans[-1]["start_time"], array_start_time)

        self.assertEqual(all_spans[-1]["start_lat"], "end_lat")
        self.assertEqual(all_spans[-1]["start_lng"], "end_lng")
        self.assertEqual(all_spans[-1]["end_lat"], "end_lat")
        self.assertEqual(all_spans[-1]["end_lng"], "end_lng")

    def test_process_spans_right_ovelap(self):
        """
        Span     x----------------------x
        Data                         x----x
        """
        # current_device_spans = self.current_device_spans

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                {  # overlaps with this span
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
            ],
        }

        array_start_time = "2019-05-22T10:47:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:52:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][-1]["spanId"]
        )
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)

        self.assertEqual(all_spans[-1]["start_lat"], "1")
        self.assertEqual(all_spans[-1]["start_lng"], "2")
        self.assertEqual(all_spans[-1]["end_lat"], "end_lat")
        self.assertEqual(all_spans[-1]["end_lng"], "end_lng")

    def test_process_spans_right_ovelap_dont_update_end_lat_long_as_they_areNone(
        self,
    ):
        """
        Dont update the end lat/lat even if we update the end_time as they are none
        Span     x----------------------x
        Data                         x----x
        """

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                {  # overlaps with this span - but dont update the end lat long
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
            ],
        }

        array_start_time = "2019-05-22T10:47:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:52:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            None,
            None,
        )

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][-1]["spanId"]
        )
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)

        self.assertEqual(all_spans[-1]["start_lat"], "1")
        self.assertEqual(all_spans[-1]["start_lng"], "2")
        self.assertEqual(all_spans[-1]["end_lat"], "3")
        self.assertEqual(all_spans[-1]["end_lng"], "4")

    def test_process_spans_first_message_with_valid_gps(self,):
        """
        The span exists but this message contains the first valid gps coordinates
        Span     x----------------------x
        Data                          x----x
        """

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {  # this span will be updated - with a correct start and end gps
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
            ],
        }

        array_start_time = "2019-05-22T10:48:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:55:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        # print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 2)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        self.assertEqual(all_spans[0]["start_lat"], "start_lat")
        self.assertEqual(all_spans[0]["start_lng"], "start_lng")
        self.assertEqual(all_spans[0]["end_lat"], "end_lat")
        self.assertEqual(all_spans[0]["end_lng"], "end_lng")

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_right_new_span(self, mock_generate_uuid):
        """
        Span     x----------------------x
        Data                                  x----x
        """

        mock_generate_uuid.return_value = "1abc"
        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
            ],
        }

        array_start_time = "2019-05-22T11:13:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T11:25:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        # print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 3)
        self.assertEqual(spanId_for_tagging, mock_generate_uuid.return_value)
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)
        self.assertEqual(all_spans[-1]["start_time"], array_start_time)

        self.assertEqual(all_spans[-1]["start_lat"], "start_lat")
        self.assertEqual(all_spans[-1]["start_lng"], "start_lng")
        self.assertEqual(all_spans[-1]["end_lat"], "end_lat")
        self.assertEqual(all_spans[-1]["end_lng"], "end_lng")

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    def test_process_spans_left_new_span(self, mock_generate_uuid):
        """
        Span             x----------------------x
        Data    x----x
        """

        mock_generate_uuid.return_value = "1abc"
        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
                # a new span will be added after this
            ],
        }

        array_start_time = "2019-05-22T09:13:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T09:25:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 3)
        self.assertEqual(spanId_for_tagging, mock_generate_uuid.return_value)
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)
        self.assertEqual(all_spans[-1]["start_time"], array_start_time)

        self.assertEqual(all_spans[-1]["start_lat"], "start_lat")
        self.assertEqual(all_spans[-1]["start_lng"], "start_lng")
        self.assertEqual(all_spans[-1]["end_lat"], "end_lat")
        self.assertEqual(all_spans[-1]["end_lng"], "end_lng")

    # @unittest.SkipTest
    def test_process_spans_left_ovelap(self):
        """
        Span        x----------------------x
        Data     x----x
        """
        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
            ],
        }

        array_start_time = "2019-05-22T10:43:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:47:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(spanId_for_tagging, "1")
        self.assertEqual(all_spans[0]["start_time"], array_start_time)

        self.assertEqual(all_spans[0]["start_lat"], "start_lat")
        self.assertEqual(all_spans[0]["start_lng"], "start_lng")
        self.assertEqual(all_spans[0]["end_lat"], "3")
        self.assertEqual(all_spans[0]["end_lng"], "4")

    def test_process_spans_gps_coord_are_none(self):
        """
        Sometimes the GPS coordinates are None
        Span              x----x
        Data     x----------------------x
        """

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {  # this span is completely inside the data
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
            ],
        }

        array_start_time = "2019-05-22T10:30:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:54:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            None,
            None,
            None,
            None,
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        # compare the start and end lat
        self.assertEqual(all_spans[0]["start_lat"], "1")
        self.assertEqual(all_spans[0]["start_lng"], "2")
        self.assertEqual(all_spans[0]["end_lat"], "3")
        self.assertEqual(all_spans[0]["end_lng"], "4")

    def test_process_spans_bigger(self):
        """
        Span              x----x
        Data     x----------------------x
        """

        current_device_spans = {
            "deviceId": "123",
            "spans": [
                {  # this span is completely inside the data
                    "spanId": "1",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T10:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T10:50:15.154000Z", DATETIME_FORMAT
                    ),
                    "start_lat": "1",
                    "start_lng": "2",
                    "end_lat": "3",
                    "end_lng": "4",
                },
                {
                    "spanId": "2",
                    "start_time": datetime.datetime.strptime(
                        "2019-05-22T12:45:05.154000Z", DATETIME_FORMAT
                    ),
                    "end_time": datetime.datetime.strptime(
                        "2019-05-22T12:50:15.154000Z", DATETIME_FORMAT
                    ),
                },
            ],
        }

        array_start_time = "2019-05-22T10:30:08.154000Z"
        dt_array_start_time = datetime.datetime.strptime(
            array_start_time, DATETIME_FORMAT
        )

        array_end_time = "2019-05-22T10:54:15.154000Z"
        dt_array_end_time = datetime.datetime.strptime(
            array_end_time, DATETIME_FORMAT
        )

        all_spans, spanId_for_tagging, modified = process_spans(
            current_device_spans["spans"],
            (array_start_time, dt_array_start_time),
            (array_end_time, dt_array_end_time),
            "start_lat",
            "start_lng",
            "end_lat",
            "end_lng",
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

        # compare the start and end lat
        self.assertEqual(all_spans[0]["start_lat"], "start_lat")
        self.assertEqual(all_spans[0]["start_lng"], "start_lng")
        self.assertEqual(all_spans[0]["end_lat"], "end_lat")
        self.assertEqual(all_spans[0]["end_lng"], "end_lng")


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCreateTimeSeriesRecord))
    suite.addTest(unittest.makeSuite(TestProcessSpans))
    return suite
