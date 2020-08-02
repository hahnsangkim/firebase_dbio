import psycopg2 as pg
from config import config
import pandas as pd
from sqlalchemy import create_engine


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = pg.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 
def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = [
        """
        CREATE TABLE stocks (
            ticker VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open MONEY NOT NULL,
            high MONEY NOT NULL,
            low MONEY NOT NULL,
            close MONEY NOT NULL,
            adj_close MONEY NOT NULL,
            volume BIGINT NOT NULL,
            dividend FLOAT NOT NULL,
            split FLOAT NOT NULL,
            logret FLOAT NOT NULL,
            ret FLOAT NOT NULL
        )
        """]
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = pg.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def delete_tables():
    """ delete a table in the PostgreSQL database"""
    commands = [
        """
        DROP TABLE IF EXISTS stocks
        """]
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = pg.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_item_list(tuple_list):
    """ insert multiple records into the stocks table  """
    sql = "INSERT INTO stocks(ticker, date, open, high, \
        low, close, adj_close, volume, dividend, split, logret, ret) \
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = pg.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,tuple_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def read_item_list(table_name):
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = pg.connect(**params)

        df = pd.read_sql_query('select * from {}'.format(table_name), con=conn)

    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()  
        return df

def get_tuple_list(ticker):
    sp = pd.read_csv('table_{}.csv'.format(ticker.lower()), index_col=False)
    sp['ticker'] = ticker.upper()
    cols = sp.columns.tolist()
    cols = [cols[-1]]+cols[:-1]
    sp = sp.reindex(columns=cols)
    tuples = list(sp.itertuples(index=False, name=None))
    return tuples

def get_csv_df(ticker):
    sp = pd.read_csv('table_{}.csv'.format(ticker.lower()), index_col=False)
    sp['ticker'] = ticker.upper()
    cols = sp.columns.tolist()
    cols = [cols[-1]]+cols[:-1]
    sp = sp.reindex(columns=cols)
    return sp

def write_to_psql(df, table_name, if_exists='append'):
    engine = None
    try:
        # read database configuration
        params = config()
        user = params['user']
        password = params['password']
        host = params['host']
        database = params['database']

        # connect to the PostgreSQL database
        uri = 'postgres://{}:{}@{}:5432/{}'.format(user, password, host, database)
        engine = create_engine(uri)

        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)

    except (Exception, pg.DatabaseError) as error:
        print(error)
    # finally:
    #     if engine is not None:
    #         engine.close() 

def read_from_psql(table_name):
    engine = None
    try:
        # read database configuration
        params = config()
        user = params['user']
        password = params['password']
        host = params['host']
        database = params['database']

        # connect to the PostgreSQL database
        uri = 'postgres://{}:{}@{}:5432/{}'.format(user, password, host, database)
        engine = create_engine(uri)

        df = pd.read_sql_query('select * from {}'.format(table_name), con=engine)

    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        return df


def read_from_psql_ticker(table_name, ticker):
    engine = None
    try:
        # read database configuration
        params = config()
        user = params['user']
        password = params['password']
        host = params['host']
        database = params['database']

        # connect to the PostgreSQL database
        uri = 'postgres://{}:{}@{}:5432/{}'.format(user, password, host, database)
        engine = create_engine(uri)
        df = pd.read_sql_query("select * from {} where ticker = '{}' order by date asc ".format(table_name, ticker.upper()), con=engine)

    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        return df

def drop_duplicates_psql(table_name):
    engine = None
    try:
        # read database configuration
        params = config()
        user = params['user']
        password = params['password']
        host = params['host']
        database = params['database']

        # connect to the PostgreSQL database
        uri = 'postgres://{}:{}@{}:5432/{}'.format(user, password, host, database)
        engine = create_engine(uri)

        df = pd.read_sql_query('select * from {}'.format(table_name), con=engine)
        df = df.drop_duplicates()

        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    except (Exception, pg.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    # connect()
    # delete_tables()
    # create_tables()

    # read a csv and write it to postresql
    # option 1
    # tuple_list = get_tuple_list('aapl')
    # insert_item_list(tuple_list)

    # option 2
    # df = get_csv_df('aapl')
    # write_to_psql(df, 'stocks')
    # df = get_csv_df('msft')
    # write_to_psql(df, 'stocks')


    df = read_from_psql('fred_rgdp')
    print(df)
    # drop_duplicates_psql('stocks')
    # df = read_from_psql('stocks')
    # print(df)

