import json
from dataclasses import asdict, dataclass, field
import string
from decimal import Decimal
from urllib3 import util
from constants import PERIOD
import logging

logger = logging.getLogger(__name__)

@dataclass (frozen = True)
class ExchangeRateEvent:
    """ A exchange rate event structure to be submitted to SQS """
    local_currency: str = field(init = True)
    exchange_rate: Decimal = field(init = True)
    exchange_rate_date: str = field(init = True)
    exchange_rate_updated_date: str = field(init = True)
    source_api_host: str = field(init = True)
    source_api_path: str = field(init = True)
    base_currency: str = field(init = True)
    period: str = field(init = True, default = PERIOD)
        
    #Validate that none of the attributes have None values because they're all required. Raise a TypeError if
    #any of the attributes are none, otherwise return the dict of the values.
    def validate_values(self):
        exchange_rates = asdict(self)
        for k,v in exchange_rates.items():
            if v == None:
                raise TypeError('The value of {} was and cannot be None!'.format(k))
                #logger.exception('The value of {} was and cannot be None!'.format(k))
            else:
                pass
        return exchange_rates
            
    #def to_sqs_message(self) -> dict:
    #Check the validity of the data and if it passes, then convert the dictionary in a utf-8 encoded json
    #to pass to SQS.
    def to_sqs_message(self):
        try:
            exchange_rates = self.validate_values()
            logger.info('Values successfully validated, creating SQS message')
            sqs_dict = json.dumps({'data':exchange_rates})#.encode('utf-8')
        except TypeError as error:
            #raise
            sqs_dict = None
            logger.exception(error)
        except Exception as error:
            sqs_dict = None
            logger.exception('Unknown exception {}'.format(error))
        return sqs_dict

    #Check the validity of the data and if it passes, then convert the dictionary in a utf-8 encoded json
    #to pass to SQS.
    def to_sql_values(self):
        try:
            #exchange_rates = self.validate_values()
            logger.info('Values successfully validated, creating SQL Values')
            sql_values = self.validate_values()
            #sql_values = exchange_rates
        except TypeError as error:
            #raise
            sql_values = None
            logger.exception(error)           
        except Exception as error:
            sql_values = None
            logger.exception('Unknown exception {}'.format(error))
        return sql_values