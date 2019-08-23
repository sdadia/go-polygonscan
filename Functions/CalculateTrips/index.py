"""

CalculateTrips:

[Function Description Here]

"""



"""

Outer Handler Area
Any code specified here (outside of handler()) will only be executed upon container initialisation.
To be used when you only want something to be executed once **not on every execution**:

 e.g. 
    Importing libraries/modules
    Service client connections
    Database connections
    etc

"""

# Module Imports
import os
import boto3

# Resource connections
dynamo_db_client = boto3.client('dynamodb')


def handler(event,context):
    """
    Inner Handler Area
    Any code specified here (inside of handler()) will be executed upon every invocation.
    To be used when you want something to be executed for every invocation:
    
    e.g. 
    Application Logic
    
    Input:
      Event:  Type Dict
              Data inputted to the Lambda function upon invocation
    
      Context:    Type Context (Object)  
                  Contains metadata regarding the current function invocation
                  
                  e.g. function_name, function_version, memory_limit_in_mb
                  Methods:    get_remaining_time_in_missis()
    
                  For more info, see:
                  https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html               
    

    Anything returned from this handler method, will be provided to the invocation source as invocation output

    """

    return 'Hello World'
