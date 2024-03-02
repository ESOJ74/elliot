import logging
import warnings
from pathlib import Path

import pandas as pd
import psycopg2 as pg
import yaml

logger = logging.getLogger()


import platform


if platform.system() == 'Windows':
    __HOME__ = Path(r'C:\Users\joseluis.cuenca\OneDrive - Bosonit\Escritorio\Elliot\pruebas')
    __TZ_HOST__ = 'Europe/Madrid'
elif platform.system() == 'Linux':
    __HOME__ = Path('/home/jose/Escritorio/Elliot/elliot')
    __TZ_HOST__ = 'UTC'

def connections():
    with open(__HOME__ / 'opdenergy.yaml', 'r') as handler:
        config = yaml.safe_load(handler)
    
    params_dic = {
        "host"      : config['bbdd_params']['host'],
        "database"  : config['bbdd_params']['database'],
        "user"      : config['bbdd_params']['user'],
        "password"  : config['bbdd_params']['password'],
        "port"      : config['bbdd_params']['port'],
        "application_name": config['bbdd_params']['application_name']
    }
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = pg.connect(**params_dic)
    except (Exception, pg.DatabaseError) as e:
        logger.info(e)    
    return conn

def select_primary_key(table: str, conn: connections) -> str:
    query = f"""select tc.table_schema, tc.table_name, kc.column_name
                from information_schema.table_constraints tc
                join information_schema.key_column_usage kc
                    on kc.table_name = tc.table_name 
                    and kc.table_schema = tc.table_schema 
                    and kc.constraint_name = tc.constraint_name
                where tc.constraint_type = 'PRIMARY KEY' and kc.table_name = '{table}'
                and kc.ordinal_position is not null
                order by tc.table_schema,
                        tc.table_name,
                        kc.position_in_unique_constraint;"""        
    
    return ','.join(pd.read_sql(query, conn)['column_name'].values)

def insert_df_to_database(df: pd.DataFrame, table: str):    
    conn=connections()
    cursor = conn.cursor()
        
    primary_key = select_primary_key(table, conn)
    print(primary_key)      
    #df.rename(columns={'signal': 'plant_id'}, inplace=True)
    
            
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    mogry = '(' + ','.join(['%s'] * len(df.columns)) + ')'   
    values = [cursor.mogrify(mogry, tup).decode('utf8') for tup in tuples]    
    updates = ["{0} = EXCLUDED.{0}".format(col)
                for col in df.columns if col not in primary_key]
    
    query = f"INSERT INTO {table} ({cols}) VALUES " + ",".join(values) + \
            f" ON CONFLICT ({primary_key}) DO UPDATE SET " + ", ".join(updates)
    
    cursor.execute(query)
    conn.commit()    

    table ='public.tmp_diego_rawdata_pruebasupsert'
    query = f"SELECT * FROM {table} order by ts desc limit 5"
    df = pd.read_sql(query, conn)
          


def insert_data(query: str) -> None:   
    conn=connections()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()        
    except Exception as e:
        raise e  
    finally:         
        cursor.close()

def fetch_data(query: str) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")   
        conn=connections()
        cursor = conn.cursor()
        try:
            df = pd.read_sql(query, conn) 
                
        except Exception as e:
            raise e
        finally:         
            cursor.close()
        return df
