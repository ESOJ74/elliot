import pandas as pd
from connection import connections

def fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='5T')
    df = df.interpolate(method='linear')
    df = df.reset_index()
    return df


def tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T')
    df = df.interpolate(method='linear')
    df = df.reset_index()
    return df


def constant_fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='5T', method='ffill')
    df = df.reset_index()
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    return df


def constant_tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T', method='ffill')
    df = df.reset_index()
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    return df


def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    instant_df = acc_df.copy()
    if freq == '5M':
        instant_df['value'] = instant_df['value'] / 12
    elif freq == '10M':
        instant_df['value'] = instant_df['value'] / 6

    if rain_var == 'Rain':
        return instant_df
    elif rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        dfs_by_day = []
        for i in range(len(forecast_days) - 1):
            group = instant_df.loc[instant_df['ts'] >= forecast_days[i]]
            group = group.loc[group['ts'] < forecast_days[i + 1]]
            group['value'] = group['value'].cumsum()
            dfs_by_day.append(group)
        result_df = pd.concat(dfs_by_day, ignore_index=True)
        return result_df


def fetch_data(query: str) -> pd.DataFrame:
    """ Coge datos desde una base de datos especificada una query"""  
    conn=connections()
    cursor = conn.cursor()
    try:
        df = pd.read_sql(query, conn)        
    except (Exception) as e:
        raise e
    cursor.close()
    return df


def insert_df_to_database(delete_query: str, df: pd.DataFrame, schema: str, table: str):
    conn=connections()
    cursor = conn.cursor()
    try:
        print('hola')
        cursor.execute(delete_query)
        conn.commit()
        print('adios')
        for i in df.index:
            cols  = ','.join(list(df.columns))
            vals  = [df.at[i,col] for col in list(df.columns)]
            query = "INSERT INTO %s(%s) VALUES('%s','%s',%s)" % (table, cols, vals[0], vals[1], vals[2])
            #cursor.execute(query)
    except (Exception) as e:
        conn.rollback()         
    cursor.close()
