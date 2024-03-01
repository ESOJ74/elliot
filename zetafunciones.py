import psycopg2 as pg
import warnings
import pandas as pd
import requests
from datetime import datetime, timedelta
from commons.SQLConnections import insert_df_to_database
import pytz
from zeta_func_aux import fiveminutal, constant_fiveminutal, accum_to_instant


def connections():   
    params_dic = {
        "host"      : "opdenergy-postgres.elliotcloud.com",
        "database"  : "opdenergy",
        "user"      : "opdenergy",
        "password"  : "tIF3J3tgIZRnvgHaVCD0",
        "port"      : 5432,
        "application_name": "meteorology-openmeteo/meteorology-weatherbit"
        }
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = pg.connect(**params_dic)
    except (Exception, pg.DatabaseError) as e:
        print(e)    
    return conn

def fetch_data(query: str) -> pd.DataFrame:
    """ Coge datos desde una base de datos especificada una query"""  
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
    
def check_meteo_signals(id_plant, lon, lat, provider):
      print("check_meteo_signals")      
      # Chequear si existe el asset de la virtual weather station de la planta
      query_asset = f"SELECT asset_id FROM assets WHERE readi_id = '{id_plant}.WS_{provider}';"
      df = fetch_data(query_asset).values[0][0]
      print(df)
      query_signals = f"SELECT id FROM public.signals WHERE signal_readi_complete ~ '{id_plant}.WS_{provider}';"
      df = fetch_data(query_signals).values[0][0]
      print(df)
      print("fin check_meteo_signals")
      return df



def captura_weatherbit(lat: float, lon: float, id_planta: str, timezone: str, exec_hour: datetime, freq: str):
    print("captura_weatherbit")
    oclock_hour = exec_hour.replace(hour=0, minute=0, second=0, microsecond=0)
    oclock_hour_post = oclock_hour + timedelta(days=1)
    five_past_twelve_hour = oclock_hour_post.replace(minute=5)

    data_url = f"https://api.weatherbit.io/v2.0/forecast/hourly?lat={lat}&lon={lon}&" \
            f"key=c42a74785e894c4fb5cd032ec6a1f4ba&hours=192"
    
    req = requests.get(data_url)
    res = req.json()
    data_list = res['data']
    

    unwanted_keys = ['app_temp', 'clouds', 'clouds_hi', 'clouds_low', 'clouds_mid', 'datetime', 'ozone', 'pod',
                     'snow_depth', 'ts', 'uv', 'vis', 'weather', 'wind_cdir', 'wind_cdir_full', 'wind_gust_spd']

    filtered_data_list = [{key: value for key, value in entry.items() if key not in unwanted_keys} for entry in
                          data_list]
    df = pd.DataFrame(filtered_data_list)   
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    df.rename(columns={'temp': 'Amb_Temp', 'dewpt': 'Dew', 'snow': 'Snow', 'wind_dir': 'Wind_Dir',
                       'wind_spd': 'Wind_Sp', 'rh': 'Humidity', 'dhi': 'Irr_Diffuse', 'dni': 'Irr_POA',
                       'ghi': 'Irr_H', 'slp': 'Atm_Press', 'precip': 'Rain_Acc'}, inplace=True)

    # Conversión de unidades
    df['Atm_Press'] = df['Atm_Press'] * 100  # conversión de hPa a Pa
    df['Snow'] = df['Snow'] / 1000  # conversión de mm a m
    
    variables = ['Amb_Temp'] #, 'Humidity', 'Dew', 'Snow', 'Atm_Press', 'Wind_Sp',
                 #'Wind_Dir', 'Irr_H', 'Irr_Diffuse', 'Irr_POA']
    
    query_rain = f"""SELECT distinct signal_readi from signals s
                    where signal_readi_complete ~ 'WS_WB.Rain'
                    and signal_readi_complete ~ '^({id_planta})';"""
    response = fetch_data(query_rain)

    rain_type = list(response['signal_readi'])    
    rain_var = 'Rain' if rain_type == [] else rain_type[0]

    variables.append(rain_var)
    df.rename(columns={'Rain_Acc': rain_var}, inplace=True)
    query = f"""SELECT id, signal_readi_complete FROM public.signals WHERE signal_readi_complete ~ '^{id_planta}.WS_WB';"""
    id_data = fetch_data(query)       
    # Optimized version
    for var in variables:
        var_df = df[['timestamp_utc', var]]
        signal_id = id_data.loc[id_data['signal_readi_complete'] == f'{id_planta}.WS_WB.{var}']['id'].values[0]
        var_df.columns = ['ts', 'value']
        df_new = constant_fiveminutal(var_df) if var == 'Rain' else fiveminutal(var_df)
        df_new['signal'] = signal_id       
        df_new = df_new[df_new['ts'] >= five_past_twelve_hour]  # Removed .loc for performance
        df_new = accum_to_instant(df_new, var, freq, five_past_twelve_hour) if 'Rain' in var else df_new
    insert_df_to_database(df_new, 'tmp_diego_curtailmentprediction1hour')
    