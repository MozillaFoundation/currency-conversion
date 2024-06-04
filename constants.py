import os

#The currency that was specified by a donor when they gave a donation to the Mozilla Foundation. A local currency can be USD.
#For the case of exchange rate API, the USD amount is not obtained because 1 USD = 1 USD will always be true.
LOCAL_CURRENCIES_LIST = [
 'AED',
 'ARS',
 'AUD',
 'AZN',
 'BAM',
 'BDT',
 'BRL',
 'CAD',
 'CHF',
 'CLP',
 'CNY',
 'CZK',
 'DKK',
 'EGP',
 'EUR',
 'GBP',
 'GEL',
 'GTQ',
 'HKD',
 'HRK',
 'HUF',
 'IDR',
 'ILS',
 'INR',
 'JPY',
 'KRW',
 'LBP',
 'MAD',
 'MXN',
 'MYR',
 'NOK',
 'NZD',
 'PHP',
 'PLN',
 'QAR',
 'RON',
 'RUB',
 'SAR',
 'SEK',
 'SGD',
 'THB',
 'TRY',
 'TWD',
 'UAH',
 'YER',
 'ZAR'
]

#Join and comma separate all the values of the LOCAL_CURRENCY_LIST and encode to use as a parameter in the API call
LOCAL_CURRENCIES = ','.join(LOCAL_CURRENCIES_LIST).encode('utf-8')

#The currency to which each Local Currency is converted to, in the case of the Mozilla Foundation, this currency is always USD.
BASE_CURRENCY = 'USD'

#The period of time for which the exchange rates are relevant, for now this is a daily midpoint
PERIOD = 'daily'

#The full URL to the CurrencyAPI resource
API_URL = 'https://api.currencyapi.com/v3/historical'

#These status codes are specific to the CurrencyAPI and can be found here https://currencyapi.com/docs/status-codes
API_STATUS_CODES = {
                     404: '404: A requested endpoint does not exist. '
                    ,422: '422: Validation error, please check the error message. '
                    ,429: '492: You have hit your rate limit or your monthly limit. '
                    ,500: '500: Internal Server Error - let us know: support@currencyapi.com. '
                    }

#SECRET_NAME = os.environ['SECRET_NAME']
#AWS_REGION = os.environ['AWS_REGION']
SALESFORCE_INSTANCE_URL = os.environ['SALESFORCE_INSTANCE_URL']
SALESFORCE_API_USER = os.environ['SALESFORCE_API_USER']
SALESFORCE_API_PASS = os.environ['SALESFORCE_API_PASS']
SALESFORCE_API_TOKEN = os.environ['SALESFORCE_API_TOKEN']

DB_URL = 'sqlite:///exchange_rates.db'