import time
import ciso8601
import json
import datetime
import logging
import os
import sys
import unittest
from pprint import pformat, pprint

os.environ["localhost"] = "1"
os.environ["OutputKinesisStreamName"] = "pvcam-ProcessedTelematicsStream-test"
os.environ[
    "SpanDynamoDBTableName"
] = "sahil_test_span_table_prefix_from_environment_var"

from pvapps_odm.Schema.models import SpanModel

SpanModel.Meta.table_name = os.environ["SpanDynamoDBTableName"] + "23112019"
# SpanModel.create_table()
# time.sleep(10)
print(SpanModel.Meta.table_name)
print("\n\n\n\n")


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
    _split_records_across_days,
    find_date_device_combos_from_records,
    get_data_for_device_from_particular_table_using_OMD,
)

SpanModel.Meta.table_name = os.environ["SpanDynamoDBTableName"] + "23112019"


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

import os
import mock
from pvapps_odm.ddbcon import dynamo_dbcon
from pynamodb.connection import Connection


SpanModel.Meta.table_name = os.environ["SpanDynamoDBTableName"] + "23112019"


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


class TestCreateTimeSeriesRecord(unittest.TestCase):

    print(SpanModel.Meta.table_name)
    ddb = dynamo_dbcon(SpanModel, Connection(host="http://localhost:8000"))
    ddb.connect()

    @unittest.SkipTest
    def test_handler(self,):
        # file_name = "small_event.json"
        file_name = "big_event.json"
        with open(file_name) as f:
            event = json.load(f)
        handler(event, None)

    def test__split_records_across_days_split_no_split(self):
        # here some records contain data across days, and other dont contain data
        # across days
        data_no_split = [
            [
                {
                    "timeStamp": "2019-11-24T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
            ]
        ]
        data_with_split = [
            [
                {
                    "timeStamp": "2019-11-23T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-23T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
            ],
            [
                {
                    "timeStamp": "2019-11-23T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-23T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "2",
                },
            ],
        ]

        output = _split_records_across_days(data_no_split + data_with_split)
        expected_output = data_no_split + [
            data_with_split[0][0:2],
            data_with_split[0][2:],
            data_with_split[1][0:2],
            data_with_split[1][2:],
        ]

        for o1, o2 in zip(expected_output, output):
            for e1, e2 in zip(o1, o2):
                self.assertEqual(e1, e2)

    def test__split_records_across_days_no_split(self):
        # there this no data to split across days in the records
        data = [
            [
                {
                    "timeStamp": "2019-11-24T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
            ]
        ]
        output = _split_records_across_days(data)
        expected_output = data

        for o1, o2 in zip(expected_output, output):
            for e1, e2 in zip(o1, o2):
                self.assertEqual(e1, e2)

    # @unittest.SkipTest
    def test__split_records_across_days(self):
        data = [
            [
                {
                    "timeStamp": "2019-11-23T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-23T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
            ],
            [
                {
                    "timeStamp": "2019-11-23T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-23T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "2",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "2",
                },
            ],
        ]

        expected_output = [data[0][0:2], data[0][2:], data[1][0:2], data[1][2:]]
        output = _split_records_across_days(data)

        for o1, o2 in zip(expected_output, output):
            for e1, e2 in zip(o1, o2):
                self.assertEqual(e1, e2)

    def test_find_date_device_combos_from_records(self):
        data_with_split = [
            [
                {
                    "timeStamp": "2019-11-24T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                }
            ],
            [
                {
                    "timeStamp": "2019-11-25T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-25T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                }
            ],
            [
                {
                    "timeStamp": "2019-11-25T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-25T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "2",
                }
            ],
        ]

        # expected_output = {"24112019": ["1"], "25112019": ["1", "2"]}
        expected_output = [
            {"date": "24112019", "deviceId": "1"},
            {"date": "25112019", "deviceId": "1"},
            {"date": "25112019", "deviceId": "2"},
        ]
        output = find_date_device_combos_from_records(data_with_split)

        # for (k1, v1), (k2, v2) in zip(output.items(), expected_output.items()):
        # self.assertEqual(k1, k2)
        # self.assertEqual(v1, v2)
        for e1, e2 in zip(output, expected_output):
            self.assertEqual(e1, e2)
        pprint(output)

    # skip as tables need to be created in advance for this test
    @unittest.SkipTest
    def test_get_data_for_device_from_particular_table_using_OMD(self):
        data = [
            {"date": "24112019", "deviceId": "1"},
            {"date": "25112019", "deviceId": "3"},
            {"date": "25112019", "deviceId": "1"},
            {"date": "25112019", "deviceId": "2"},
        ]
        ans = get_data_for_device_from_particular_table_using_OMD(data)
        pprint(ans)

    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.update_modified_device_spans_in_dynamo_using_ODM"
    )
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_all_records_in_event"
    )
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_data_for_device_from_particular_table_using_OMD"
    )
    def test_handler_mock(
        self,
        mock_get_data_from_odm,
        mock_get_all_records_in_event,
        mock_update_date_in_dynamo,
    ):
        mock_get_all_records_in_event.return_value = [
            [
                {
                    "timeStamp": "2019-11-23T14:11:02.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:02.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-23T14:11:03.003Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T14:11:03.000Z",
                        "lat": "53.337238N",
                        "lng": "6.233203W",
                        "speed": "5.06",
                        "course": "256.320",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:04.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:04.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
                {
                    "timeStamp": "2019-11-24T14:11:14.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T14:11:14.000Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "1",
                },
            ],
            [
                {
                    "timeStamp": "2019-11-24T23:59:44.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T23:59:44.006Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "3",
                },
                {
                    "timeStamp": "2019-11-24T23:59:54.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-24T23:59:54.006Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "3",
                },
                {
                    "timeStamp": "2019-11-25T00:00:14.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-25T00:00:14.006Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "3",
                },
                {
                    "timeStamp": "2019-11-25T00:00:24.006Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-25T00:00:24.006Z",
                        "lat": "53.337237N",
                        "lng": "6.233223W",
                        "speed": "4.54",
                        "course": "261.480",
                    },
                    "deviceId": "3",
                },
            ],
        ]
        mock_get_data_from_odm.return_value = {
            "25112019": {"3": {"deviceId": [], "spans": []}},
            "23112019": {"1": {"deviceId": [], "spans": []}},
            "24112019": {
                "1": {"deviceId": [], "spans": []},
                "3": {"deviceId": [], "spans": []},
            },
        }
        mock_update_date_in_dynamo.return_value = "successfull"

        handler(None, None)

    def test_update_modified_device_spans_in_dynamo_using_ODM(self):
        data = {
            "24112019": {"1": {"deviceId": "1", "spans": []}},
            "25112019": {
                "1": {"deviceId": "1", "spans": []},
                "2": {"deviceId": "2", "spans": []},
                "3": {"deviceId": "3", "spans": []},
            },
        }
        update_modified_device_spans_in_dynamo_using_ODM(data)

        output = get_data_for_device_from_particular_table_using_OMD(
            [
                {"date": "24112019", "deviceId": "1"},
                {"date": "25112019", "deviceId": "1"},
                {"date": "25112019", "deviceId": "2"},
                {"date": "25112019", "deviceId": "3"},
            ]
        )
        pprint(output)
        expected_output = {
            "24112019": {"1": {"deviceId": "1", "spans": []}},
            "25112019": {
                "1": {"deviceId": "1", "spans": []},
                "2": {"deviceId": "2", "spans": []},
                "3": {"deviceId": "3", "spans": []},
            },
        }
        for k1, k2 in zip(sorted(output), sorted(expected_output)):
            self.assertEqual(k1, k2)
            print(k1, k2)
            for e1, e2 in zip(sorted(output[k1]), sorted(expected_output[k2])):
                print(output[k1][e1], expected_output[k2][e2])
                self.assertEqual(output[k1][e1], expected_output[k2][e2])

    @mock.patch("Functions.CreateTimeseriesRecord.index.generate_uuid")
    @mock.patch(
        "Functions.CreateTimeseriesRecord.index.get_all_records_in_event"
    )
    def test_backfill_data_goes_to_correct_span(
        self, mock_get_all_records_in_event, mock_generate_uuid
    ):
        # SpanModel.Meta.table_name = os.environ["SpanDynamoDBTableName"] + "23112019"
        # SpanModel.create_table()
        # time.sleep(10)
        # 8 to 8 :10                                  8:29 to 8:32
        # 8:20 to 8:25 - 8:26 to 8:28
        mock_generate_uuid.side_effect = ["span_id_1", "span_id_2", "span_id_3"]
        mock_get_all_records_in_event.return_value = [
            [  # 8:00 am to 8:10 am
                {
                    "timeStamp": "2019-11-23T08:00:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:00:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
                {
                    "timeStamp": "2019-11-23T08:10:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:10:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
            ],
            [  # 8:26 to 8:28 am
                {
                    "timeStamp": "2019-11-23T08:26:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:26:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
                {
                    "timeStamp": "2019-11-23T08:28:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:28:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
            ],
            [  # 8:20 to 8:25 am
                {
                    "timeStamp": "2019-11-23T08:20:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:20:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
                {
                    "timeStamp": "2019-11-23T08:25:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:25:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
            ],
            [  # 8:29 to 8:32 am  -- back data
                {
                    "timeStamp": "2019-11-23T08:29:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:29:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
                {
                    "timeStamp": "2019-11-23T08:32:00.000Z",
                    "acc": "1",
                    "io": "11111111",
                    "gps": {
                        "status": "valid",
                        "gpsTime": "2019-11-23T08:32:00.000Z",
                        "lat": "53.337240N",
                        "lng": "6.233178W",
                        "speed": "7.45",
                        "course": "259.760",
                    },
                    "deviceId": "4",
                },
            ],
        ]
        handler(None, None)

        spans = get_data_for_device_from_particular_table_using_OMD(
            [{"date": "23112019", "deviceId": "4"}]
        )
        expected_spans = {
            "23112019": {
                "4": {
                    "deviceId": "4",
                    "spans": [
                        {
                            "end_time": datetime.datetime(
                                2019,
                                11,
                                23,
                                8,
                                10,
                                tzinfo=datetime.timezone.utc,
                            ),
                            "spanId": "span_id_1",
                            "start_time": datetime.datetime(
                                2019, 11, 23, 8, 0, tzinfo=datetime.timezone.utc
                            ),
                        },
                        {
                            "end_time": datetime.datetime(
                                2019,
                                11,
                                23,
                                8,
                                32,
                                tzinfo=datetime.timezone.utc,
                            ),
                            "spanId": "span_id_2",
                            "start_time": datetime.datetime(
                                2019,
                                11,
                                23,
                                8,
                                20,
                                tzinfo=datetime.timezone.utc,
                            ),
                        },
                    ],
                }
            }
        }

        for e1, e2 in zip(
            spans["23112019"]["4"]["spans"],
            expected_spans["23112019"]["4"]["spans"],
        ):
            self.assertEqual(e1, e2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestCreateTimeSeriesRecord))
    # suite.addTest(unittest.makeSuite(TestProcessSpans))
    return suite
