import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from commons.SQLConnections import fetch_data, insert_df_to_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def captura_weatherbit(
        lat: float, lon: float, id_planta: str, timezone: str, exec_hour: datetime, freq: str):

    oclock_hour = exec_hour.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    oclock_hour_post = oclock_hour + timedelta(days=1)
    five_past_twelve_hour = oclock_hour_post.replace(minute=5)

    data_url = f"https://api.weatherbit.io/v2.0/forecast/hourly?lat={lat}&lon={lon}&" \
            f"key=c42a74785e894c4fb5cd032ec6a1f4ba&hours=192"

    res = requests.get(data_url).json()
    data_list = res['data']

    unwanted_keys = ['app_temp', 'clouds', 'clouds_hi', 'clouds_low', 'clouds_mid',
                     'datetime', 'ozone', 'pod', 'snow_depth', 'ts', 'uv', 'vis',
                     'weather', 'wind_cdir', 'wind_cdir_full', 'wind_gust_spd']

    filtered_data_list = [{key: value for key, value in entry.items()
                           if key not in unwanted_keys} for entry in
                           data_list]
    df = pd.DataFrame(filtered_data_list)
    
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    df.rename(columns={'temp': 'Amb_Temp', 'dewpt': 'Dew', 'snow': 'Snow',
                       'wind_dir': 'Wind_Dir', 'wind_spd': 'Wind_Sp',
                       'rh': 'Humidity', 'dhi': 'Irr_Diffuse', 'dni': 'Irr_POA',
                       'ghi': 'Irr_H', 'slp': 'Atm_Press', 'precip': 'Rain_Acc'},
                       inplace=True)

    # Conversión de unidades
    df['Atm_Press'] = df['Atm_Press'] * 100  # conversión de hPa a Pa
    df['Snow'] = df['Snow'] / 1000  # conversión de mm a m
    
    query_rain = f"""SELECT distinct signal_readi from signals s
                    where signal_readi_complete ~ 'WS_WB.Rain'
                    and signal_readi_complete ~ '^({id_planta})';"""
    response = fetch_data(query_rain)
    rain_type = list(response['signal_readi'])
    rain_var = rain_type[0] if rain_type else "Rain"      
    df.rename(columns={'Rain_Acc': rain_var}, inplace=True)

    query = f"""SELECT id, signal_readi_complete
                FROM public.signals
                WHERE signal_readi_complete ~ '^{id_planta}.WS_WB';"""
    id_data = fetch_data(query)

    variables = ['Amb_Temp', 'Humidity', 'Dew', 'Snow', 'Atm_Press', 'Wind_Sp',
                 'Wind_Dir', 'Irr_H', 'Irr_Diffuse', 'Irr_POA', rain_var]
    
    freq_funcs = {
                "5M": (constant_fiveminutal if rain_var == "Rain" else fiveminutal),
                "10M": (constant_tenminutal if rain_var == "Rain" else tenminutal)
            }
    
    for var in variables:
        var_df = df[['timestamp_utc', var]]
        signal_id = id_data.loc[id_data['signal_readi_complete'] == f'{id_planta}.WS_WB.{var}']['id'].values[0]

        var_df.columns = ['ts', 'value']
        df_new = freq_funcs[freq](var_df)
        df_new["signal"] = signal_id

        sub_df = df_new.loc[df_new['ts'] >= five_past_twelve_hour]
        if var == rain_var:            
            sub_df = accum_to_instant(sub_df, var, freq, five_past_twelve_hour)        
        
        print(sub_df)
        #insert_df_to_database(sub_df, 'tmp_diego_rawdata_pruebasupsert')


def fiveminutal(df):
    return df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()


def tenminutal(df):
    return df.set_index('ts').asfreq('10T').interpolate('linear').reset_index()


def constant_fiveminutal(df):
    primer_valor = df['value'][0] 
    return df.set_index('ts').asfreq('5T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))


def constant_tenminutal(df):
    primer_valor = df['value'][0]    
    return df.set_index('ts').asfreq('10T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))


def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    instant_df = acc_df.copy()
    
    divisor = 12 if freq == '5M' else 6
    instant_df['value'] /= divisor
    
    if rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        dfs_by_day = []
        for i in range(len(forecast_days) - 1):            
            group = instant_df.loc[instant_df['ts'] >= forecast_days[i]]
            group = group.loc[group['ts'] < forecast_days[i + 1]]            
            group['value'] = group['value'].cumsum()
            dfs_by_day.append(group)  
        return pd.concat(dfs_by_day, ignore_index=True)
    return instant_df
