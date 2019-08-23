import sys
import os

"""
Following line used to add a path to the projects base directory to the python runtime, allowing for references to 'Functions' directory

Current Sys Path Example:
..../trip-calculation/Tests/UnitTests/test_create_timeseries_record.py

New Sys Path Example:
..../trip-calculation
"""
sys.path.append( os.path.dirname( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) )

import unittest
import json
import mock
from Functions.CreateTimeseriesRecord import index

import boto3
import botocore

# Mocked Environment Variables for the function
env_vars = {
    'SpanTable':'span_mock',
    'TimeseriesTable':'ts_mock'
}


@mock.patch.dict('os.environ',env_vars)
class TestHandlerCase(unittest.TestCase):
    from Functions.CreateTimeseriesRecord import index
    successful_event = {
        
        "Hello":"World"
        
    }

    error_event = {
        
        "Hello":"Goodbye"
        
    }

    boto3_event = {
        
        "Hello":"World",
        "boto3":"True"
        
    }

    def mock_make_successful_api_call(self, operation_name, kwarg):
        orig = botocore.client.BaseClient._make_api_call
        # operation_name = API Being Called By boto3 (e.g. GetItem, Query)   
        # kwarg = Arguements being provided with the API Call (e.g. TableName)

        if operation_name == 'GetItem':
            
            if kwarg.get('TableName') == env_vars['SpanTable']:
                response = {
                    'Item': {
                        'SpanId': {
                            'S': '123'
                        }
                    }
                }
              
            return response
            
           
        return orig(self, operation_name, kwarg)

    @mock.patch('Functions.CreateTimeseriesRecord.index.application_logic_example_method',wraps=index.application_logic_example_method)
    def test_successful_invocation(self, mock_app_logic_method):
           
        print("============================================")
        print("CreateTimeseriesRecord - test_successful_invocation")
        print("============================================")

        # Clone sample function input
        event = dict(self.successful_event)
        
        # Invoke function with input
        response = index.handler(event,None)
        
        self.assertEqual(response,'Hello World')
        mock_app_logic_method.assert_called_with('hello')

    @mock.patch('Functions.CreateTimeseriesRecord.index.application_logic_example_method',wraps=index.application_logic_example_method)
    def test_failed_invocation(self, mock_app_logic_method):
           
        print("============================================")
        print("CreateTimeseriesRecord - test_failed_invocation")
        print("============================================")

        # Clone sample function input
        event = dict(self.error_event)
        
        # Invoke function with input
        with self.assertRaises(Exception):
            
            try:
                index.handler(event,None)
            except Exception as e:
                self.assertEqual('My Exception Message',str(e))
                raise

        self.assertFalse(mock_app_logic_method.called)


    @mock.patch('Functions.CreateTimeseriesRecord.index.application_logic_example_method',wraps=index.application_logic_example_method)
    @mock.patch("botocore.client.BaseClient._make_api_call",new=mock_make_successful_api_call) 
    def test_with_boto3_mock(self, mock_app_logic_method):
           
        print("============================================")
        print("CreateTimeseriesRecord - test_with_boto3_mock")
        print("============================================")

        # Clone sample function input
        event = dict(self.boto3_event)
        
        # Invoke function with input
        response = index.handler(event,None)
        
        # Response is the result of the mocked boto3 api call to DynamoDB
        self.assertEqual(response,'123')
        mock_app_logic_method.assert_called_with('hello')