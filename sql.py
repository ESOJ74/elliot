from connection import connections
import pandas as pd
from funciones import *
import requests
import pytz
from datetime import datetime, timedelta
import warnings

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
     
with warnings.catch_warnings():
    warnings.simplefilter("ignore") 
    conn=connections()
    cursor = conn.cursor()
    table ='tmp_diego_curtailmentprediction1hour'
    primary_key = select_primary_key(table, conn)
    #print(primary_key)

    query = f"SELECT * FROM {table} order by ts desc limit 5"
    df = pd.read_sql(query, conn)
    #print(df)
    df['value']=50
    

    start_date = df['ts'].values[0]
    end_date = df['ts'].values[len(df) - 1]

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    values = [cursor.mogrify("(%s,%s,%s)", tup).decode('utf8') for tup in tuples]

    # Corrección aquí: formato adecuado para la cláusula DO UPDATE SET
    updates = ["{0} = EXCLUDED.{0}".format(col) for col in df.columns if col not in primary_key]
    #print(updates)

    query = f"INSERT INTO {table} ({cols}) VALUES " + ",".join(values) + \
            f" ON CONFLICT ({primary_key}) DO UPDATE SET " + ", ".join(updates)

    """cursor.execute(query)
    conn.commit()"""
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    
    #print(df)
    #print(query)
