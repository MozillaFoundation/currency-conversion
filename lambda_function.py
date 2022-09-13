import json
import boto3
from ExchangeRateEvent import *
from get_secrets import *
from get_currencies import *
from app_database import *
from sqs import *
from constants import SECRET_NAME, AWS_REGION, API_URL, BASE_CURRENCY, LOCAL_CURRENCIES
import logging

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    secrets = get_secret(SECRET_NAME, AWS_REGION)
    
    QUEUE_URL = secrets['QUEUE_URL']
    API_KEY = secrets['API_KEY']
    DB_URL = secrets['DB_URL']
    
    try:
        exchange_rates = get_currencies(
                                        BASE_CURRENCY
                                        ,LOCAL_CURRENCIES
                                        ,['2022-02-09'] ##This needs to be dynamic
                                        ,API_KEY
                                        ,API_URL
                                        )
        
        #As long as the exchange_rates data is not None then try and transact it                        
        if exchange_rates:
            try:
                logger.info('Successfully connected to API Endpoint. Processing exchange_rates data.')
                sql_values = [e.to_sql_values() for e in exchange_rates]
        
                sqs_messages = [e.to_sqs_message()  for e in exchange_rates]
                
                #Iterate through the list of SQS messages and send them one at a time
                for m in sqs_messages:
                    send_sqs_message(QUEUE_URL, m)
                logger.info('Successfully sent {} messages to the SQS queue.'.format(len(sqs_messages)))
            
                insert_exchange_rate(DB_URL, 'exchange_rates', sql_values)
                logger.info('Successfully inserted {} rows to the SQL table.'.format(len(sql_values)))
            
            except Exception as error:
                logger.exception('Error transcating exchange_rates data with SQL or SQS!')
                logger.exception(error)
                
    except Exception as error:
        logger.exception('Error connecting to API Endpoint!')
        logger.exception(error)