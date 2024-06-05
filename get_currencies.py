import urllib3
from urllib3 import util
import json
from ExchangeRateEvent import *
from constants import API_STATUS_CODES
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_failed_response(response, fail_info):
    
    all_details = []
    
    info = response.get(fail_info)
    
    if isinstance(info, dict):
        for k,v in info.items():
            all_details.append('Error in {} parameters/fields, {}. '.format(k 
                                                                            ,''.join(v).lower())
                                                                            )
    else:
        all_details.append(str(info) + ' ')
        
    return ' '.join(all_details)

def check_response_status(response):
    try:
        response_status = response.status
        
     #If the response is successful, then parse each element in the response data and convert it to an object
     #of the class ExchangeRateEvent. Append each of these objects to the exchange_rates list. 
        if response_status == 200:
            logging.info('Successful request to the exchange rate API endpoint.')
            
            #Extract the rate limits and quotas from the header for logging purposes
            quota_limit = int(response.headers['x-ratelimit-limit-quota-month'])
            quota_used = int(response.headers['x-ratelimit-remaining-quota-month'])
            quote_pct = 1.0 - (quota_used/quota_limit)
            
            #print('After this request, {} of {} requests have been made against our quota for a utilization percentage of {}'
            #      .format(quota_used, quota_limit, quote_pct))
            logging.info('After this request, {} of {} requests have been made against our quota for a utilization percentage of {}'
                           .format(quota_used, quota_limit, quote_pct))
            
        else:
            #Lookup the generic failure response information for adding to the HTTPError message
            generic = API_STATUS_CODES.get(response_status)
            
            #Parse any error information for adding to the HTTPError message
            errors = parse_failed_response(response_status, 'errors')
            
            #Parse any message information for adding to the HTTPError message
            messages = parse_failed_response(response_status, 'message')
                
            #Combine all of the information together when raising the error
            #raise urllib3.exceptions.HTTPError(str(API_STATUS_CODES[response_status]) 
            #                                   + str(errors) 
            #                                   + str(messages)
            #                                   + 'input date: {}.'.format(date))
            http_exception = urllib3.exceptions.HTTPError(str(API_STATUS_CODES[response_status]) 
                                                          + str(errors) 
                                                          + str(messages)
                                                          + 'input date: {}.'.format(date))
            logger.exception(http_exception)
            
    except Exception as error:
        response = None
        logging.exception(error)
        
    return response

def get_currencies(base_currency, currencies, dates, api_key, api_url):
    
    #Parse the API URL and create the additiona metadata about the API to store with the data
    api_url_parsed = util.parse_url(api_url)
    #These are not constants because they are generated from the API URL
    SOURCE_API_HOST = api_url_parsed.host
    SOURCE_API_PATH = api_url_parsed.path

    #Make sure the dates are the appropriate instance of object, either a string or a list.
    if isinstance(dates, list):
        dates = dates
    elif isinstance(dates, str):
        dates = list(dates.split())
    else:
        #raise TypeError('dates must be a list or a string')
        logger.exception('Dates must be a list or a string')
        
    http = urllib3.PoolManager()
    exchange_rates = []
    
    #Iterate through the inputed dates
    for date in dates:
        try:
            response = http.request(url = api_url
                                     ,method = 'GET'
                                     ,headers = {'apikey': api_key}
                                     ,fields = {'date': date
                                                ,'base_currency': base_currency
                                                ,'currencies': currencies})
            
            checked_response = check_response_status(response)
            
            #If a successful response was returned and processed, then return it for further handling
            if checked_response:
                response_data = json.loads(checked_response.data)
            
            #If the response is successful, then parse each element in the response data and convert it to an object
            #of the class ExchangeRateEvent. Append each of these objects to the exchange_rates list.         
                for k,v in response_data['data'].items():
                    exchange_rates.append(ExchangeRateEvent(local_currency = v['code']
                                                            ,exchange_rate = v['value']
                                                            ,exchange_rate_updated_date= response_data['meta']['last_updated_at']
                                                            ,exchange_rate_date = date
                                                            ,source_api_host = SOURCE_API_HOST
                                                            ,source_api_path = SOURCE_API_PATH
                                                            ,base_currency = base_currency)) 
            
        except Exception as error:
            logger.exception(error)
            exchange_rates = None
        
    #Return the list of dataclass objects 
    return exchange_rates