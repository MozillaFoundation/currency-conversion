import boto3
import base64
from botocore.exceptions import ClientError
import json
import logging

logger = logging.getLogger(__name__)

# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/
def get_secret(secret_name, region_name):


    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    #https://docs.aws.amazon.com/secretsmanager/latest/userguide/retrieving-secrets_cache-python.html
    #See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        secret = None
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.exception(e)
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.exception(e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.exception(e)
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.exception(e)
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.exception(e)
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            #secret = json.loads(get_secret_value_response['SecretString'])
            secret = json.loads(get_secret_value_response['SecretString'])
        else:
            #secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))
            secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))
            
    return secret