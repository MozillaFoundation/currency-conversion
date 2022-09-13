import boto3
from constants import AWS_REGION
import logging

logger = logging.getLogger(__name__)


def send_sqs_message(queue_url, data):
    try:
        session = boto3.session.Session()
        
        client = session.client(
                                service_name = 'sqs'
                                ,region_name = AWS_REGION
                               )
        
        if queue_url and data:
            msg_response = client.send_message(QueueUrl = queue_url
                                               ,MessageBody = data)
        else: 
            logger.exception('Either the queue_url is invalid the data is invalid. {}.')
                         
    except Exception as error:
        logger.exception(error)