import pytz
import json
import datetime
import logging
import os
import sys
import unittest
from pprint import pformat, pprint

from pvapps_odm.Schema.models import SpanModel

os.environ["OutputKinesisStreamName"] = "pvcam-ProcessedTelematicsStream-test"

logging.getLogger("index").setLevel(logging.DEBUG)

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
    generate_uuid,
    DATETIME_FORMAT,
)


logger = logging.getLogger("test_span_calculator2")
logger.setLevel(logging.DEBUG)

import os
import mock


class TestCreateTimeSeriesRecord(unittest.TestCase):
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


class TestProcessSpans(unittest.TestCase):
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
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

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
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(all_spans[0]["end_time"], array_end_time)

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
        )
        # print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 3)
        self.assertEqual(spanId_for_tagging, mock_generate_uuid.return_value)
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)
        self.assertEqual(all_spans[-1]["start_time"], array_start_time)

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
        )
        # print("{} {} {}".format(all_spans, spanId_for_tagging, modified))

        self.assertEqual(modified, True)
        self.assertEqual(len(all_spans), 3)
        self.assertEqual(spanId_for_tagging, mock_generate_uuid.return_value)
        self.assertEqual(all_spans[-1]["end_time"], array_end_time)
        self.assertEqual(all_spans[-1]["start_time"], array_start_time)

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
        )
        print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(spanId_for_tagging, "1")
        self.assertEqual(all_spans[0]["start_time"], array_start_time)

    def test_process_spans_bigger(self):
        """
        Data              x----x
        Span     x----------------------x
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
        )
        # print(all_spans, spanId_for_tagging, modified)

        self.assertEqual(modified, True)
        self.assertEqual(
            spanId_for_tagging, current_device_spans["spans"][0]["spanId"]
        )
        self.assertEqual(all_spans[0]["start_time"], array_start_time)
        self.assertEqual(all_spans[0]["end_time"], array_end_time)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCreateTimeSeriesRecord))
    return suite
