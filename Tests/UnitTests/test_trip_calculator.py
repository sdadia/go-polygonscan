from unittest import mock
import ciso8601
import numpy as np

# import numpy.nan as nan
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
os.environ["SpanDynamoDBTableName"] = "pvcam-prod-TripCalculation-SpanTable-"
# os.environ["SpanDynamoDBTableName"] = "Trip-calc"
os.environ["AggregateTable"] = "mango"


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

from pvapps_odm.Schema.models import (
    SpanModel,
    AggregationModel,
    StationaryIdlingModel,
)
from Functions.CalculateTrips.index import (
    aggregate_stationary_idling_time,
    keep_relevant_data_for_stationary_idling_btw_start_end_time,
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
            "start_lat": "10_start_lat",
            "start_lng": "10_start_lng",
            "end_lat": "10_end_lat",
            "end_lng": "10_end_lng",
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
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            2: {
                "end_time": "2019-11-26 08:20:00+00:00",
                "spanId": ["0", "1"],
                "start_time": "2019-11-26 08:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            3: {
                "end_time": "2019-11-26 09:20:00+00:00",
                "spanId": ["2", "3", "4"],
                "start_time": "2019-11-26 08:43:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            4: {
                "end_time": "2019-11-26 10:20:00+00:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            5: {
                "end_time": "2019-11-26 11:20:00+00:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            6: {
                "end_time": "2019-11-26 14:20:00+00:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            7: {
                "end_time": "2019-11-26 16:20:00+00:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            8: {
                "end_time": "2019-11-26 23:20:00+00:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "10_end_lat",
                "end_lng": "10_end_lng",
            },
        }

        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00Z",
            user_end_time="2019-11-26 23:59:59Z",
            time_diff_between_spans=10,
        )
        for tripid_1, tripid_2 in zip(ans, expected_output):
            self.assertEqual(ans[tripid_1], expected_output[tripid_2])

        # self.assertDictEqual(ans, expected_output)

    def test_different_time_difference_between_spans(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        logging.info("Testing Different Trip Seperation Time")

        time_diff = 30  # minutes
        ans = get_trips_pandas(
            self.data,
            user_start_time="2019-11-26 00:00:00Z",
            user_end_time="2019-11-26 23:59:59Z",
            time_diff_between_spans=time_diff,
        )

        expected_output = {
            1: {
                "end_time": "2019-11-26 07:12:00+00:00",
                "spanId": ["-1"],
                "start_time": "2019-11-26 06:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            2: {
                "end_time": "2019-11-26 09:20:00+00:00",
                "spanId": ["0", "1", "2", "3", "4"],
                "start_time": "2019-11-26 08:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            3: {
                "end_time": "2019-11-26 10:20:00+00:00",
                "spanId": ["5"],
                "start_time": "2019-11-26 10:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            4: {
                "end_time": "2019-11-26 11:20:00+00:00",
                "spanId": ["6"],
                "start_time": "2019-11-26 11:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            5: {
                "end_time": "2019-11-26 14:20:00+00:00",
                "spanId": ["7"],
                "start_time": "2019-11-26 13:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            6: {
                "end_time": "2019-11-26 16:20:00+00:00",
                "spanId": ["8"],
                "start_time": "2019-11-26 16:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "",
                "end_lng": "",
            },
            7: {
                "end_time": "2019-11-26 23:20:00+00:00",
                "spanId": ["9", "10"],
                "start_time": "2019-11-26 20:00:00+00:00",
                "start_lat": "",
                "start_lng": "",
                "end_lat": "10_end_lat",
                "end_lng": "10_end_lng",
            },
        }

        self.assertLessEqual(len(ans), len(self.data))
        self.assertDictEqual(ans, expected_output)

    # @unittest.SkipTest
    def test_handler(self):
        # with open("sample_trip_calculation_input.json") as f:
        # event = json.load(f)
        event = {
            "deviceId": "11adaffa-3b2e-4b4f-ba6b-a0b5ed38c551",
            "start_datetime": "2019-12-19 00:00:00Z",
            "end_datetime": "2019-12-19 23:59:59Z",
            "trip_time_diff": 10,
        }
        trips = handler(event, None)

    @unittest.SkipTest
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

    def test_keep_relevant_data_for_stationary_idling_btw_start_end_time(self):
        data = {
            "time": [
                1554569100.0,  # 2019-04-06 16:45:00,  0
                1554569160.0,  # 2019-04-06 16:46:00,  1
                1554569220.0,  # 2019-04-06 16:47:00,  0
                1554569280.0,  # 2019-04-06 16:48:00,  1
                1554569340.0,  # 2019-04-06 16:49:00,  0
                1554569400.0,  # 2019-04-06 16:50:00,  1
            ],
            "curr": [0, 1, 0, 1, 0, 1],
        }

        trimmed_data = keep_relevant_data_for_stationary_idling_btw_start_end_time(
            "2019-04-06 16:47:00", "2019-04-06 16:49:00", data.copy()
        )
        for x, y in zip(range(len(trimmed_data)), [2, 3, 4]):
            self.assertEqual(trimmed_data["time"][x], data["time"][y])
            self.assertEqual(trimmed_data["curr"][x], data["curr"][y])

        trimmed_data = keep_relevant_data_for_stationary_idling_btw_start_end_time(
            "2019-04-06 16:51:00", "2019-04-06 16:52:00", data.copy()
        )
        self.assertEqual(trimmed_data["time"], [])
        self.assertEqual(trimmed_data["curr"], [])

    def test_get_trips_pandas_bigger(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        """
        Spans     x-----x   
        Query  y....................................y
        Output : keep this span
        """

        ans = get_trips_pandas(
            self.data,
            user_start_time=("2019-11-26 07:58:00Z"),
            user_end_time=("2019-11-26 08:13:00Z"),
            time_diff_between_spans=10,
        )
        expected_output = {
            1: {
                "end_lat": "",
                "end_lng": "",
                "end_time": "2019-11-26 08:12:00+00:00",
                "spanId": ["0"],
                "start_lat": "",
                "start_lng": "",
                "start_time": "2019-11-26 08:00:00+00:00",
            }
        }

        self.assertEqual(ans, expected_output)

    def test_get_trips_pandas_left_overlap(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        """
        Spans     x-----x   
        Query  y.....y
        Output : keep this span
        """

        ans = get_trips_pandas(
            self.data,
            user_start_time=("2019-11-26 07:58:00Z"),
            user_end_time=("2019-11-26 08:04:00Z"),
            time_diff_between_spans=10,
        )
        expected_output = {
            1: {
                "end_lat": "",
                "end_lng": "",
                "end_time": "2019-11-26 08:12:00+00:00",
                "spanId": ["0"],
                "start_lat": "",
                "start_lng": "",
                "start_time": "2019-11-26 08:00:00+00:00",
            }
        }

        self.assertEqual(ans, expected_output)

    def test_get_trips_pandas_right_overlap(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        """
        Spans     x-----x   
        Query        y........y
        Output : keep this span
        """

        ans = get_trips_pandas(
            self.data,
            user_start_time=("2019-11-26 08:04:00Z"),
            user_end_time=("2019-11-26 08:13:00Z"),
            time_diff_between_spans=10,
        )
        expected_output = {
            1: {
                "end_lat": "",
                "end_lng": "",
                "end_time": "2019-11-26 08:12:00+00:00",
                "spanId": ["0"],
                "start_lat": "",
                "start_lng": "",
                "start_time": "2019-11-26 08:00:00+00:00",
            }
        }

        self.assertEqual(ans, expected_output)

    def test_get_trips_pandas_completely_inside(self):
        from Functions.CalculateTrips.index import get_trips_pandas

        """
        Spans     x--------x   
        Query        y...y
        Output : keep this span
        """

        ans = get_trips_pandas(
            self.data,
            user_start_time=("2019-11-26 08:04:00Z"),
            user_end_time=("2019-11-26 08:06:00Z"),
            time_diff_between_spans=10,
        )
        expected_output = {
            1: {
                "end_lat": "",
                "end_lng": "",
                "end_time": "2019-11-26 08:12:00+00:00",
                "spanId": ["0"],
                "start_lat": "",
                "start_lng": "",
                "start_time": "2019-11-26 08:00:00+00:00",
            }
        }

        self.assertEqual(ans, expected_output)

    @mock.patch("Functions.CalculateTrips.index.ddb_stationary_idling")
    def test_aggregate_stationary_idling_for_trip(
        self, mock_ddb_stationary_idling
    ):
        data_for_mock = {
            "deviceId": "123",
            "stationary_state_transition": json.dumps(
                {
                    "prev": [-1, 0, 1, 0, 1, 0],
                    "time": [
                        1554569100.0,  # 2019-04-06 16:45:00,  0
                        1554569160.0,  # 2019-04-06 16:46:00,  1
                        1554569220.0,  # 2019-04-06 16:47:00,  0
                        1554569280.0,  # 2019-04-06 16:48:00,  1
                        1554569340.0,  # 2019-04-06 16:49:00,  0
                        1554569400.0,  # 2019-04-06 16:50:00,  1
                    ],
                    "curr": [0, 1, 0, 1, 0, 1],
                }
            ),
            "idling_state_transition": json.dumps(
                {
                    "prev": [-1, 0, 1, 0, 1, 0],
                    "time": [
                        1554569100.0,  # 2019-04-06 16:45:00,  0
                        1554569160.0,  # 2019-04-06 16:46:00,  1
                        1554569220.0,  # 2019-04-06 16:47:00,  0
                        1554569280.0,  # 2019-04-06 16:48:00,  1
                        1554569340.0,  # 2019-04-06 16:49:00,  0
                        1554569400.0,  # 2019-04-06 16:50:00,  1
                    ],
                    "curr": [0, 1, 0, 1, 0, 1],
                }
            ),
        }

        mock_ddb_stationary_idling.get_object.return_value = StationaryIdlingModel(
            **data_for_mock
        )

        stationry_idling_time_dict = aggregate_stationary_idling_time(
            "123", "2019-04-06 16:44:00", "2019-04-06 16:50:00"
        )
        self.assertEqual(stationry_idling_time_dict["stationary_time"], 120)
        self.assertEqual(stationry_idling_time_dict["idling_time"], 120)

        stationry_idling_time_dict = aggregate_stationary_idling_time(
            "123", "2019-04-06 16:51:00", "2019-04-06 16:52:00"
        )
        self.assertEqual(stationry_idling_time_dict["stationary_time"], -1)
        self.assertEqual(stationry_idling_time_dict["idling_time"], -1)

        stationry_idling_time_dict = aggregate_stationary_idling_time(
            "123", "2019-04-06 16:41:00", "2019-04-06 16:46:00"
        )
        self.assertEqual(stationry_idling_time_dict["stationary_time"], 0)
        self.assertEqual(stationry_idling_time_dict["idling_time"], 0)

    @unittest.SkipTest
    @mock.patch("Functions.CalculateTrips.index.ddb_stationary_idling")
    @mock.patch("Functions.CalculateTrips.index.ddb_agg")
    @mock.patch("Functions.CalculateTrips.index.ddb_span")
    def test_handler_mock(
        self, mock_ddb_spans, mock_ddb_agg, mock_ddb_stationary_idling
    ):
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

        data_for_mock = {
            "deviceId": "123",
            "stationary_state_transition": json.dumps(
                {
                    "prev": [-1, 0, 1, 0, 1, 0],
                    "time": [
                        1558543500.0,  # 2019-05-22 16:45:00,  0
                        1558543560.0,  # 2019-05-22 16:46:00,  1
                        1558543620.0,  # 2019-05-22 16:47:00,  0
                        1558543680.0,  # 2019-05-22 16:48:00,  1
                        1558543740.0,  # 2019-05-22 16:49:00,  0
                        1558543800.0,  # 2019-05-22 16:50:00,  1
                    ],
                    "curr": [0, 1, 0, 1, 0, 1],
                }
            ),
            "idling_state_transition": json.dumps(
                {
                    "prev": [-1, 0, 1, 0, 1, 0],
                    "time": [
                        1558543500.0,  # 2019-05-22 16:45:00,  0
                        1558543560.0,  # 2019-05-22 16:46:00,  1
                        1558543620.0,  # 2019-05-22 16:47:00,  0
                        1558543680.0,  # 2019-05-22 16:48:00,  1
                        1558543740.0,  # 2019-05-22 16:49:00,  0
                        1558543800.0,  # 2019-05-22 16:50:00,  1
                    ],
                    "curr": [0, 1, 0, 1, 0, 1],
                }
            ),
        }

        mock_ddb_stationary_idling.get_object.return_value = StationaryIdlingModel(
            **data_for_mock
        )

        event = {
            "deviceId": "123",
            "start_datetime": "2019-05-22 00:00:00",
            "end_datetime": "2019-05-22 23:59:59",
            "trip_time_diff": 10,
        }
        ans = handler(event, None)
        expected_output = {
            "trips": [
                {
                    "end_time": "2019-05-22 10:47:06.154000+00:00",
                    "start_lat": "",
                    "start_lng": "",
                    "end_lat": "",
                    "end_lng": "",
                    "metrics": {
                        "avg_speed": 10.46,
                        "idling_time": -1,
                        "stationary_time": -1,
                    },
                    "spanId": ["1234"],
                    "start_time": "2019-05-22 10:45:05.154000+00:00",
                },
                {
                    "end_time": "2019-05-22 11:47:06.154000+00:00",
                    "start_lat": "",
                    "start_lng": "",
                    "end_lat": "",
                    "end_lng": "",
                    "metrics": {
                        "avg_speed": 10.46,
                        "idling_time": -1,
                        "stationary_time": -1,
                    },
                    "spanId": ["2234"],
                    "start_time": "2019-05-22 11:45:05.154000+00:00",
                },
                {
                    "end_time": "2019-05-22 16:47:06.154000+00:00",
                    "start_lat": "",
                    "start_lng": "",
                    "end_lat": "",
                    "end_lng": "",
                    "metrics": {
                        "avg_speed": 10.46,
                        "idling_time": 66.15,
                        "stationary_time": 66.15,
                    },
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
