from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
import logging

logger = logging.getLogger(__name__)

def create_db_engine(db_url):
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(autocommit=True, bind=engine)
        
    except Exception as error:
        logger.exception('Exception encountered when creating session and engine. {}'.format(error))
        engine = None
        Session = None
        
    return engine, Session

def insert_exchange_rate(db_url, table, data):
    #Create the session and the engine
    engine, in_session = create_db_engine(db_url)
    
    #As long as the engine and session are not None, attempt the connection
    if engine and in_session:
        with in_session.begin() as session:
            try:
                #Get the Metadata from the database using the engine 
                metadata = MetaData(engine)

                #Using the metdata connect to the desired table and autoload it for use in column checking and inserting
                target_table = Table(table, metadata, autoload = True)

                #Create sets of the table columns and the input data columns and compare if there is a symmetric difference
                #between these two columns sets. If not, then proceed to inserting the data, otherwise return an error or 
                #warning.
                table_cols = set([c.name for c in target_table.columns])

            except Exception as error:
                table_cols = None
                logger.exception('Failed to connect to databse and/or table object. {}'.format(error))
                
            if data:
                data_cols = set([k for v in data for k in v.keys()])

            if table_cols and data_cols:
                sym_diff = table_cols.symmetric_difference(data_cols)
                
                #Log an exception if a column difference is found.
                if sym_diff:
                    logger.exception('Column difference detected between SQL Table {} and data.'.format(table))
                    logger.excpetion('Aborting table insert!')

                ##Need to determine if it's better to iterate through records and do individual inserts in the event that an
                ##insert for a particular record fails. If there are no column differences then construct and execute the insert
                else:
                    try:
                        insert_stmt = (
                                       target_table.insert()
                                       .values(data)
                                      )

                        session.execute(insert_stmt)
                        logger.info('Successfully inserted {} records into {}.'.format(len(data), table))

                    except Exception as error:
                        logger.exception('Table insert failed. {}'.format(error))