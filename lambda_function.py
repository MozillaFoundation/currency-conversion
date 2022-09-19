import json
import boto3
from ExchangeRateEvent import *
from get_secrets import *
from get_currencies import *
from app_database import *
from sqs import *
from constants import SECRET_NAME, AWS_REGION, API_URL, BASE_CURRENCY, LOCAL_CURRENCIES
import logging
from datetime import timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

secrets = get_secret(SECRET_NAME, AWS_REGION)
QUEUE_URL = secrets['QUEUE_URL']
API_KEY = secrets['API_KEY']
DB_URL = secrets['DB_URL']
    

engine = connect_db_engine(DB_URL)

def lambda_handler(event, context):

    try:
        vw_latest_exchange_rate = latest_exchange_rate(engine, 'vw_latest_exchange_rate')
        
        if vw_latest_exchange_rate:
            next_date = [(vw_latest_exchange_rate[0][0] + timedelta(days = 1)).strftime('%Y-%m-%d')]
            logger.info('Computed {} as the next date to retrieve exchange rate data.'.format(next_date))
        
        if next_date:
            exchange_rates = get_currencies(
                                            BASE_CURRENCY
                                            ,LOCAL_CURRENCIES
                                            ,next_date
                                            ,API_KEY
                                            ,API_URL
                                            )
            logger.info('Sending API request for {}.'.format(next_date))
        
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
            
                #insert_exchange_rate(engine, 'exchange_rates', sql_values)
                logger.info('Successfully inserted {} rows to the SQL table.'.format(len(sql_values)))
            
            except Exception as error:
                logger.exception('Error transcating exchange_rates data with SQL or SQS!')
                logger.exception(error)
                
    except Exception as error:
        logger.exception('Error connecting to API Endpoint!')
        logger.exception(error)