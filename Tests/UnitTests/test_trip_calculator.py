import mock
import logging
import os
import sys
import unittest
from pprint import pprint, pformat
import json

# this contains the code for trip calculation
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

logging.getLogger("m2").setLevel(logging.ERROR)

from Functions.CalculateTrips.index import (
    AggregationModel,
    SpanModel,
    aggregate_speed_for_trip,
    handler,
)


# mock environment variables
class Test_trip_calculator_pandas(unittest.TestCase):

    data = [
        {
            "spanId": "-1",
            "start_time": "2019-11-26T06:00:00.00Z",
            "end_time": "2019-11-26T07:12:00Z",
        },
        {
            "spanId": "1",
            "start_time": "2019-11-26T08:14:00Z",
            "end_time": "2019-11-26T08:20:00Z",
        },
        {
            "spanId": "3",
            "start_time": "2019-11-26T08:48:00Z",
            "end_time": "2019-11-26T08:58:00Z",
        },
        {
            "spanId": "0",
            "start_time": "2019-11-26T08:00:00Z",
            "end_time": "2019-11-26T08:12:00Z",
        },
        {
            "spanId": "4",
            "start_time": "2019-11-26T09:04:00Z",
            "end_time": "2019-11-26T09:20:00Z",
        },
        {
            "spanId": "5",
            "start_time": "2019-11-26T10:00:00Z",
            "end_time": "2019-11-26T10:20:00Z",
        },
        {
            "spanId": "6",
            "start_time": "2019-11-26T11:00:00Z",
            "end_time": "2019-11-26T11:20:00Z",
        },
        {
            "spanId": "7",
            "start_time": "2019-11-26T13:00:00Z",
            "end_time": "2019-11-26T14:20:00Z",
        },
        {
            "spanId": "9",
            "start_time": "2019-11-26T20:00:00Z",
            "end_time": "2019-11-26T22:20:00Z",
        },
        {
            "spanId": "8",
            "start_time": "2019-11-26T16:00:00Z",
            "end_time": "2019-11-26T16:20:00Z",
        },
        {
            "spanId": "10",
            "start_time": "2019-11-26T22:22:00Z",
            "end_time": "2019-11-26T23:20:00Z",
        },
        {
            "spanId": "2",
            "start_time": "2019-11-26T08:43:00Z",
            "end_time": "2019-11-26T08:46:00Z",
        },
    ]

    def test_default_settings(self):
        """
        Check if we get the expected output in the trip dictionary
        """
        from Functions.CalculateTrips.index import get_trips_pandas

        expected_output = {
            1: {
                "end_time": "2019-11-26 07:12:00+00:00",
                "spanId": ["-1"],
                "start_time": "2019-11-26 06:00:00+00:00",
            },
            2: {
                "end_time": "2019-11-26 08:20:00+00:00",
                "spanId": ["0", "1"],
                "start_time": "2019-11-26 08:00:00+00:00",
            },
            3: {
                "end_time": "2019-11-26 09:20:00+00:00",
                "spanId": ["2", "3", "4"],
                "start_time": "2019-11-26 08:43:00+00:00",
            },
            4: {
                "end_time": "2019-11-26 10:20:00+00:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00+00:00",
            },
            5: {
                "end_time": "2019-11-26 11:20:00+00:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00+00:00",
            },
            6: {
                "end_time": "2019-11-26 14:20:00+00:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00+00:00",
            },
            7: {
                "end_time": "2019-11-26 16:20:00+00:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00+00:00",
            },
            8: {
                "end_time": "2019-11-26 23:20:00+00:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00+00:00",
            },
        }

        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00",
            user_end_time="2019-11-26 23:59:59",
            time_diff_between_spans=10,
        )
        # pprint(ans)

        self.assertDictEqual(ans, expected_output)

    def test_different_time_difference_between_spans(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        logging.info("Testing Different Trip Seperation Time")

        time_diff = 30  # minutes
        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00",
            user_end_time="2019-11-26 23:59:59",
            time_diff_between_spans=time_diff,
        )

        expected_output = {
            1: {
                "end_time": "2019-11-26 07:12:00+00:00",
                "spanId": ["-1"],
                "start_time": "2019-11-26 06:00:00+00:00",
            },
            2: {
                "end_time": "2019-11-26 09:20:00+00:00",
                "spanId": ["0", "1", "2", "3", "4"],
                "start_time": "2019-11-26 08:00:00+00:00",
            },
            3: {
                "end_time": "2019-11-26 10:20:00+00:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00+00:00",
            },
            4: {
                "end_time": "2019-11-26 11:20:00+00:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00+00:00",
            },
            5: {
                "end_time": "2019-11-26 14:20:00+00:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00+00:00",
            },
            6: {
                "end_time": "2019-11-26 16:20:00+00:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00+00:00",
            },
            7: {
                "end_time": "2019-11-26 23:20:00+00:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00+00:00",
            },
        }

        self.assertLessEqual(len(ans), len(self.data))
        self.assertDictEqual(ans, expected_output)

    @unittest.SkipTest
    def test_handler(self):
        from Functions.CalculateTrips.index import handler

        with open("./sample_trip_calculation_input.json") as f:
            event = json.load(f)

        trips = handler(event, None)
        pprint(trips)

    @mock.patch("Functions.CalculateTrips.index.ddb_agg")
    def test_aggregate_speed_for_trip(self, mock_ddb_agg):
        data_for_mock = [
            {
                "spanId_metricname": "1234_speed",
                "timestamp": "2019-04-06 16:01:00",
                "value": 11,
                "count": 1,
            },
            {
                "spanId_metricname": "1234_speed",
                "timestamp": "2019-04-06 16:02:00",
                "value": 10,
                "count": 1,
            },
        ]

        mock_ddb_agg.session.query.side_effect = [
            [AggregationModel(**d) for d in data_for_mock],
            [],
        ]
        # print(mock_ddb_agg)
        # print(mock_ddb_agg.session.query.side_effect)

        ans = aggregate_speed_for_trip(["1234", "1235"])
        expected_answer = {"avg_speed": 10.5}
        self.assertEqual(ans, expected_answer)

    @mock.patch("Functions.CalculateTrips.index.ddb_agg")
    @mock.patch("Functions.CalculateTrips.index.ddb_span")
    def test_handler_mock(self, mock_ddb_spans, mock_ddb_agg):
        mock_ddb_span_data = {
            "deviceId": "123",
            "spans": json.dumps(
                [
                    {
                        "spanId": "1234",
                        "start_time": "2019-05-22T10:45:05.154000Z",
                        "end_time": "2019-05-22T10:47:06.154000Z",
                    },
                    {
                        "spanId": "2234",
                        "start_time": "2019-05-22T11:45:05.154000Z",
                        "end_time": "2019-05-22T11:47:06.154000Z",
                    },
                    {
                        "spanId": "3234",
                        "start_time": "2019-05-22T16:45:05.154000Z",
                        "end_time": "2019-05-22T16:47:06.154000Z",
                    },
                ]
            ),
        }
        mock_ddb_spans.batch_get.return_value = [
            SpanModel(**mock_ddb_span_data)
        ]

        mock_data_for_agg = [
            [
                {
                    "spanId_metricname": "1234_speed",
                    "timestamp": "2019-04-06 10:45:00",
                    "value": 11,
                    "count": 6,
                },
                {
                    "spanId_metricname": "1234_speed",
                    "timestamp": "2019-04-06 10:46:00",
                    "value": 10,
                    "count": 6,
                },
                {
                    "spanId_metricname": "1234_speed",
                    "timestamp": "2019-04-06 10:47:00",
                    "value": 10,
                    "count": 1,
                },
            ],
            [
                {
                    "spanId_metricname": "2234_speed",
                    "timestamp": "2019-04-06 11:45:00",
                    "value": 11,
                    "count": 6,
                },
                {
                    "spanId_metricname": "2234_speed",
                    "timestamp": "2019-04-06 11:46:00",
                    "value": 10,
                    "count": 6,
                },
                {
                    "spanId_metricname": "2234_speed",
                    "timestamp": "2019-04-06 11:47:00",
                    "value": 10,
                    "count": 1,
                },
            ],
            [
                {
                    "spanId_metricname": "3234_speed",
                    "timestamp": "2019-04-06 16:45:00",
                    "value": 11,
                    "count": 6,
                },
                {
                    "spanId_metricname": "3234_speed",
                    "timestamp": "2019-04-06 16:46:00",
                    "value": 10,
                    "count": 6,
                },
                {
                    "spanId_metricname": "3234_speed",
                    "timestamp": "2019-04-06 16:47:00",
                    "value": 10,
                    "count": 1,
                },
            ],
        ]
        mock_ddb_agg.session.query.side_effect = [
            [AggregationModel(**d) for d in mock_data_for_agg[0]],
            [AggregationModel(**d) for d in mock_data_for_agg[1]],
            [AggregationModel(**d) for d in mock_data_for_agg[2]],
        ]

        event = {
            "deviceId": "123",
            "start_datetime": "2019-05-22 00:00:00",
            "end_datetime": "2019-05-22 23:59:59",
            "trip_time_diff": 10,
        }
        ans = handler(event, None)
        ans = json.loads(ans)
        expected_output = {
            "trips": [
                {
                    "end_time": "2019-05-22 10:47:06.154000+00:00",
                    "metrics": {"avg_speed": 10.46},
                    "spanId": ["1234"],
                    "start_time": "2019-05-22 10:45:05.154000+00:00",
                },
                {
                    "end_time": "2019-05-22 11:47:06.154000+00:00",
                    "metrics": {"avg_speed": 10.46},
                    "spanId": ["2234"],
                    "start_time": "2019-05-22 11:45:05.154000+00:00",
                },
                {
                    "end_time": "2019-05-22 16:47:06.154000+00:00",
                    "metrics": {"avg_speed": 10.46},
                    "spanId": ["3234"],
                    "start_time": "2019-05-22 16:45:05.154000+00:00",
                },
            ]
        }

        for e1, e2 in zip(expected_output["trips"], ans["trips"]):
            self.assertEqual(e1, e2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_trip_calculator_pandas))
    return suite
