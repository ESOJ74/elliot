import warnings
import pandas as pd
import psycopg2 as pg
from psycopg2 import extras
import yaml


def connections():   
    with open('energy_prices_prueba/energyprices-opdenergy.yaml', 'r') as handler:
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
        print(e)    
    return conn

def select_primary_key(table: str) -> str:  
    query = """SELECT kc.column_name
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kc
                   ON kc.table_name = tc.table_name 
                   AND kc.table_schema = tc.table_schema 
                   AND kc.constraint_name = tc.constraint_name
               WHERE tc.constraint_type = 'PRIMARY KEY' 
                   AND kc.table_name = %s
                   AND kc.ordinal_position IS NOT NULL
               ORDER BY tc.table_schema,
                        tc.table_name,
                        kc.position_in_unique_constraint;"""       
    cursor = connections().cursor()
    cursor.execute(query, (table,))
    primary_keys = cursor.fetchall()
    cursor.close()
    return ','.join(pk[0] for pk in primary_keys)


def insert_df_to_database(df: pd.DataFrame, table: str):
    conn = connections()   
    cursor = conn.cursor()
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    primary_key = select_primary_key(table)
    updates = [f"{col} = EXCLUDED.{col}" for col in df.columns if col not in primary_key]
    update_statement = ", ".join(updates)
    query = f"""INSERT INTO {table} ({cols}) VALUES %s
                ON CONFLICT ({primary_key}) DO UPDATE SET {update_statement}"""

    extras.execute_values(cursor, query, tuples, template=None, page_size=100)
    conn.commit()
    cursor.close()
    conn.close()  


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






