import psycopg2 as pg
import pandas as pd
import logging
import yaml
from pathlib import Path
import warnings
logger = logging.getLogger()


import platform
from pathlib import Path

if platform.system() == 'Windows':
    __HOME__ = Path(r'C:\Users\joseluis.cuenca\OneDrive - Bosonit\Escritorio\Elliot\pruebas')
    __TZ_HOST__ = 'Europe/Madrid'
elif platform.system() == 'Linux':
    __HOME__ = Path('/opt/meteorology')
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
       
    prymary_key=','.join(pd.read_sql(query, conn)['column_name'].values)
    return prymary_key

def insert_df_to_database(df: pd.DataFrame, table: str):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore") 
        conn=connections()
        cursor = conn.cursor()
         
        primary_key = select_primary_key(table, conn)
        print(primary_key)    
         
        df.rename(columns={'signal': 'plant_id'}, inplace=True)
        print(df)
              
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        values = [cursor.mogrify("(%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        # Corrección aquí: formato adecuado para la cláusula DO UPDATE SET
        updates = ["{0} = EXCLUDED.{0}".format(col) for col in df.columns if col not in primary_key]
        
        query = f"INSERT INTO {table} ({cols}) VALUES " + ",".join(values) + \
                f" ON CONFLICT ({primary_key}) DO UPDATE SET " + ", ".join(updates)
        cursor.execute(query)
        conn.commit()    

        table ='tmp_diego_curtailmentprediction1hour'
        query = f"SELECT * FROM {table} order by ts desc limit 5"
        df = pd.read_sql(query, conn)
        print('----------------------------------')
        print(df)   


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