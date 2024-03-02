from connection import connections
import pandas as pd
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
    return  ','.join(pd.read_sql(query, conn)['column_name'].values)
     

conn=connections()
cursor = conn.cursor()
table ='tmp_diego_rawdata_pruebasupsert'
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
table='raw_data'
query = f"SELECT * FROM {table} where signal='147832' order by ts desc limit 50"
"""cursor.execute(query)
conn.commit()"""
from datetime import datetime
def comprobar(modo):
    table='raw_data'
    table = 'tmp_diego_rawdata_pruebasupsert'
    query = f"SELECT * FROM {table} where signal='147832' order by ts desc limit 50"

    query = f"""SELECT * FROM tmp_diego_rawdata_pruebasupsert order by ts {modo},signal limit 100;"""
    df = pd.read_sql(query, conn)
    print(df)
    start_date = df['ts'].values[0]
    start_date = str(start_date).split('T')
    start_date = ' '.join(start_date).split('.')
    start_date = datetime.strptime(f'{start_date[0]}+00:00', '%Y-%m-%d %H:%M:%S%z')
    print(start_date)
    end_date = df['ts'].values[len(df)-1]  
    end_date = str(end_date).split('T')
    end_date = ' '.join(end_date).split('.')
    end_date = datetime.strptime(f'{end_date[0]}+00:00', '%Y-%m-%d %H:%M:%S%z')
    print(end_date)
    query = f"select FROM public.raw_data " \
            f"WHERE ts > '2024-01-17'  limit 1000"
    
    query = f"""SELECT * FROM tmp_diego_rawdata_pruebasupsert AS a
                JOIN raw_data AS b
                ON b.signal = CAST(a.signal AS INTEGER)
                and a.ts=b.ts
                order by a.ts asc
                LIMIT 100;"""
    print(query)
    df2 = pd.read_sql(query, conn)
    print(df2)
    #merged_df = df.merge(df2, indicator=True, how='outer')
    #print(merged_df[merged_df['_merge']!='both'])

comprobar('asc')