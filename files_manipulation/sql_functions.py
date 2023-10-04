from dotenv import dotenv_values
import pandas as pd
import sqlalchemy
import mysql.connector 
from sqlalchemy import text

def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values(".env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    return sql_config

# Insert the get_engine() function definition below - when instructed
def get_engine_alchemy():

    engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@host/database',
                    connect_args=get_sql_config())
    return engine

def get_engine_mysql():
    engine = mysql.connector.connect(**get_sql_config())
    return engine

## engine same like connection
def get_data_mysql(query):
    engine = get_engine_mysql()

    cursor = engine.cursor()
    cursor.execute(query)
    results = []
    for i, data in enumerate(cursor):
        results.append(data)
    cursor.close()
    engine.close()
    return results


# Insert the get_data() function definition below - do this only when instructed in the notebook
def get_data_alchemy(query):
    engine = get_engine_alchemy()
    query = text(query)
    with engine.begin() as conn:
        results = conn.execute(query)
        return results.fetchall()
    



# Insert the get_dataframe() function definition below - do this only when instructed in the notebook
def get_dataframe(query):
    engine = get_engine_alchemy()
    
    return pd.read_sql_query(sql=query, con = engine)