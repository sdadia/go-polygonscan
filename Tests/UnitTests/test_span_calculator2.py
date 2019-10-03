import json
import datetime
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
    get_all_records_in_event,
    remove_invalid_trip_data,
)

logger = logging.getLogger("test_span_calculator2")
logger.setLevel(logging.INFO)

import os
import mock

os.environ["OutputKinesisStreamName"] = "dan_dax_output"


class TestCreateTimeSeriesRecord(unittest.TestCase):
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_spans_for_devices_from_DAX_batch_usingODM"
    )
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_all_records_in_event"
    )
    def test_handler(
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
                        "start_time": datetime.datetime(
                            2019, 5, 22, 10, 45, 5, 154000
                        ),
                        "end_time": datetime.datetime(
                            2019, 5, 22, 10, 45, 14, 154000
                        ),
                        "spanId": "123",
                    }
                ],
                "deviceId": "9b59fd3e-17e0-11e9-ab14-d663bd873",
            }
        ]
        logger.info("Testing handler")
        # with open("./sample_span_calculation_input.json") as f:
        # event = json.load(f)
        # logger.debug("Event is : {}".format(event))
        records = handler({}, None)
        logger.info("Extracted records are :\n {}".format(pformat(records)))

        logger.info("Testing handler...Done")

    def test_get_all_records_in_event(self):
        logging.info("Testing get_all_records_in_event function")
        with open("./sample_span_calculation_input.json") as f:
            event = json.load(f)
        logger.debug("Event is : {}".format(event))
        all_records = get_all_records_in_event(event)

        logging.debug(
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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCreateTimeSeriesRecord))
    return suite
