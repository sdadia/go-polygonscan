import logging
import datetime
from dateutil.tz import tzutc
import mock
import os
import sys
import unittest
import ciso8601
import json
from pprint import pprint, pformat


os.environ["localhost"] = "1"
log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


# this contains the code for trip calculation
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from Functions.SpeedAggregations.index import (
    handler,
    extract_data_from_kinesis,
    find_unique_span_timestamp,
    format_event_data,
    update_data_in_dynamo_using_ODM,
)


from pvapps_odm.Schema.models import AggregationModel
from pvapps_odm.ddbcon import Connection
from pvapps_odm.ddbcon import dynamo_dbcon

pprint(logging.log)


class Test_speed_aggregation(unittest.TestCase):
    # @unittest.SkipTest
    def test_handler(self):
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        logging.debug("Event is :\n{}".format(pformat(event)))
        handler(event, None)

    # @mock.patch(
    # "Functions.SpeedAggregations.index.update_data_in_dynamo_using_ODM"
    # )
    @mock.patch(
        "Functions.SpeedAggregations.index.get_metric_data_from_dynamo_batch_from_ODM_table_read"
    )
    @mock.patch("Functions.SpeedAggregations.index.extract_data_from_kinesis")
    def test_handler_mock(
        self,
        mock_extract_data_from_kinesis,
        mock_get_data_from_ODM,
        # mock_update_data_in_dyamo,
    ):
        # mock_update_data_in_dyamo.side_effect = [[], []]
        mock_extract_data_from_kinesis.side_effect = [
            [
                {
                    "spanId": "123",
                    "speed": "0.28",
                    "timestamp": "2019-05-22T10:45:05.154000Z",
                },
                {
                    "spanId": "123",
                    "speed": "0.83",
                    "timestamp": "2019-05-22T10:45:06.154000Z",
                },
                {
                    "spanId": "123",
                    "speed": "0.52",
                    "timestamp": "2019-05-22T10:45:07.154000Z",
                },
            ],
            [
                {
                    "spanId": "123",
                    "speed": "0.28",
                    "timestamp": "2019-05-22T10:45:05.154000Z",
                },
                {
                    "spanId": "123",
                    "speed": "0.83",
                    "timestamp": "2019-05-22T10:45:06.154000Z",
                },
                {
                    "spanId": "123",
                    "speed": "0.52",
                    "timestamp": "2019-05-22T10:45:07.154000Z",
                },
            ],
        ]
        mock_get_data_from_ODM.side_effect = [
            [],
            [
                {
                    "count": 3,
                    "spanId_metricname": "123_speed",
                    "speed": 0.5433333333333332,
                    "timestamp": datetime.datetime(
                        2019, 5, 22, 10, 45, tzinfo=tzutc()
                    ),
                }
            ],
        ]
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        logging.debug("Event is :\n{}".format(pformat(event)))
        handler(event, None)
        handler(event, None)

        if "localhost" in os.environ:
            ddb_test = dynamo_dbcon(
                AggregationModel, conn=Connection("http://localhost:8000")
            )
            ddb_test.connect()
        else:
            ddb_test = dynamo_dbcon(AggregationModel, conn=Connection())
            ddb_test.connect()

        response = ddb_test.session.query("123_speed", None)[0].attribute_values
        print(response)
        response["value"] = round(response["value"], 5)

        expected_response = {
            "count": 6,
            "spanId_metricname": "123_speed",
            "value": 0.54333,
            "timestamp": datetime.datetime(2019, 5, 22, 10, 45, tzinfo=tzutc()),
        }
        self.assertEqual(response, expected_response)

    def test_extract_data_from_kinesis(self):
        logging.info("Testing extract data from kinesis")
        with open("./sample_speed_aggregation_input_event.json") as f:
            event = json.load(f)
        log.debug("Event is :\n{}".format(pformat(event)))
        ans = extract_data_from_kinesis(event)

        expected_response = [
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

        for e1, e2 in zip(ans, expected_response):
            self.assertCountEqual(e1, e2)

        logging.info("Testing extract data from kinesis...Done")

    def test_find_unique_span_timestamp(self):
        input_data = [
            {"spanId": "123", "timestamp": "2019-05-22 10:45:07.154000"},
            {"spanId": "123", "timestamp": "2019-05-22 10:45:07.154000"},
            {"spanId": "122", "timestamp": "2019-05-22 10:45:07.154000"},
        ]
        ans = find_unique_span_timestamp(input_data)

        expected_response = [
            ("123", "2019-05-22 10:45:07.154000"),
            ("122", "2019-05-22 10:45:07.154000"),
        ]

        for e1, e2 in zip(sorted(ans), sorted(expected_response)):
            self.assertEqual(e1, e2)

    def test_format_event_data(self):
        input_data = [
            {
                "spanId": "123",
                "timestamp": "2019-05-22 10:46:07.154000+00:00",
                "speed": 120,
            },
            {
                "spanId": "123",
                "timestamp": "2019-05-22 10:45:07.154000+00:00",
                "speed": 160,
            },
            {
                "spanId": "122",
                "timestamp": "2019-05-22 10:45:07.154000+00:00",
                "speed": 280,
            },
        ]

        expected_output = [
            {
                "timestamp": "2019-05-22 10:46:00+00:00",
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:00+00:00",
                "spanId_metricname": "123_speed",
                "speed": 160.0,
                "count": 1.0,
            },
            {
                "timestamp": "2019-05-22 10:45:00+00:00",
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 1.0,
            },
        ]
        event_df = format_event_data(input_data)

        for e1, e2 in zip(
            expected_output, list(event_df.to_dict(orient="index").values())
        ):
            # print(e1, e2)
            self.assertEqual(e1, e2)

    @mock.patch("Functions.SpeedAggregations.index.ddb.session")
    def test_update_data_in_dynamo_using_ODM(self, mock_ddb_session_commit):
        mock_ddb_session_commit.commit.return_value = True
        data = [
            {
                "timestamp": str(
                    ciso8601.parse_datetime("2019-05-22T10:44:07Z")
                ),
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": str(
                    ciso8601.parse_datetime("2019-05-22T10:44:07Z")
                ),
                "spanId_metricname": "123_speed",
                "speed": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": str(
                    ciso8601.parse_datetime("2019-05-22T10:45:07Z")
                ),
                "spanId_metricname": "123_speed",
                "speed": 160.0,
                "count": 2.0,
            },
            {
                "timestamp": str(
                    ciso8601.parse_datetime("2019-05-22T10:46:07Z")
                ),
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 3.0,
            },
            {
                "timestamp": str(
                    ciso8601.parse_datetime("2019-05-22T10:46:07Z")
                ),
                "spanId_metricname": "122_speed",
                "speed": 280.0,
                "count": 3.0,
            },
        ]

        expected_output = [
            {
                "timestamp": (ciso8601.parse_datetime("2019-05-22T10:44:00Z")),
                "spanId_metricname": "123_speed",
                "value": 120.0,
                "count": 1.0,
            },
            {
                "timestamp": (ciso8601.parse_datetime("2019-05-22T10:45:00Z")),
                "spanId_metricname": "123_speed",
                "value": 160.0,
                "count": 2.0,
            },
            {
                "timestamp": (ciso8601.parse_datetime("2019-05-22T10:46:00Z")),
                "spanId_metricname": "122_speed",
                "value": 280.0,
                "count": 3.0,
            },
        ]
        ans = update_data_in_dynamo_using_ODM(data)
        pprint(ans)
        ans = [x.attribute_values for x in ans]
        for e1, e2 in zip(expected_output, ans):
            # print(e1, e2)
            # e2["timestamp"] = str(e2["timestamp"])
            self.assertEqual(e1, e2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_speed_aggregation))
    return suite
