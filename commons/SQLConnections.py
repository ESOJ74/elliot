import warnings

import pandas as pd
import psycopg2 as pg
from psycopg2 import extras
import yaml


def connections():   
    with open('opdenergy.yaml', 'r') as handler:
        config = yaml.safe_load(handler)   
    try:
        conn = pg.connect(**config['bbdd_params'])
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


def insert_df_to_database2(df: pd.DataFrame, table: str):
    conn=connections()   
    cursor = conn.cursor()
    primary_key = select_primary_key(table)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    placeholders = '(' + ','.join(['%s'] * len(df.columns)) + ')'
    values = [cursor.mogrify(placeholders, tup).decode('utf8') for tup in tuples]
    updates = [f"{col} = EXCLUDED.{col}" for col in df.columns if col not in primary_key]

    query = f"INSERT INTO {table} ({cols}) VALUES " + ",".join(values) + \
            f" ON CONFLICT ({primary_key}) DO UPDATE SET " + ", ".join(updates)
    cursor.execute(query)
    conn.commit()
    cursor.close()


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
