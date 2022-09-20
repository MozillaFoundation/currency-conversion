from sqlalchemy import MetaData, Table, select, create_engine
from sqlalchemy.engine.base import Engine, Connection
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_db_engine(db_url) -> Engine:
    try:
        engine = create_engine(db_url)
        logger.info('Successfully created database engine')
        
    except Exception as error:
        logger.exception('Exception encountered when creating engine. {}'.format(error))
        engine = None
        
    return engine

def connect_db_engine(db_url) -> Connection:
    engine = create_db_engine(db_url)
    
    if engine:
        try:
            engine = engine.connect()
            logger.info('Successfully connected to database engine.')
        except:
            logger.exception('Failed to connect to database. {}'.format(error))
            
    return engine
            
        
def connect_db_object(connected_engine, object_name) -> Table:
    try:
        metadata = MetaData(connected_engine)
        logger.info('Successfully retrieved Metadata from the database.')
        
        target_object = Table(object_name, metadata, autoload = True)
        logger.info('Successfully connected to the {} object in the database.'.format(object_name))
        
    except Exception as error:
        metadata = None
        
        target_object = None
        
        logger.exception('Failed to connect to the {} object in the database. {}'.format(object_name, error))
    return target_object
        
        
def insert_exchange_rate(connected_engine, object_name, data):
    try:
        target_object = connect_db_object(connected_engine, object_name)

        table_cols = set([c.name for c in target_object.columns])
        logger.info('Extracting columns from database table.')

        if data:
            data_cols = set([k for v in data for k in v.keys()])
            logger.info('Extracting columns from the API data.')

        if table_cols and data_cols:
            logger.info('Comparing the columns between the database table and the API data.')
            sym_diff = table_cols.symmetric_difference(data_cols)

        #Log an exception if a column difference is found.
        if sym_diff:
            logger.exception('Column difference detected between SQL Table {} and data.'.format(target_object.name))
            logger.exception('Aborting table insert!')

        ##Need to determine if it's better to iterate through records and do individual inserts in the event that an
        ##insert for a particular record fails. If there are no column differences then construct and execute the insert
        else:
            try:
                insert_stmt = (
                               target_object.insert()
                               .values(data)
                              )

                connected_engine.execute(insert_stmt)
                logger.info('Successfully inserted {} records into {}.'.format(len(data), target_object.name))

            except Exception as error:
                logger.exception('Table insert failed. {}'.format(error))
                
    except Exception as error:
        logger.exception('Failed to connect to database or object'.format(error))
                        
def latest_exchange_rate(connected_engine, object_name):
    try:
        target_object = connect_db_object(connected_engine, object_name)
        
        select_stmt = select([target_object])

        results = connected_engine.execute(select_stmt)

        results = results.fetchall()
        
    except Exception as error:
        results = None
        logger.exception('Failed to connect to database and/or table object. {}'.format(error))
    return results