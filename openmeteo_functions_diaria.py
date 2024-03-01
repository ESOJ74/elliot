import pandas as pd
import logging
import requests
import pytz
from datetime import datetime, timedelta
from commons.SQLConnections import fetch_data, insert_df_to_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def captura_openmeteo_diaria(lat: float, lon: float, exec_hour: datetime, freq: str, id_planta: str):

    oclock_hour = exec_hour.replace(minute=0, second=0, microsecond=0)  # dejar la hora solamente
    tendays_ago = datetime.today() + timedelta(days=-10)

    if oclock_hour.date() < tendays_ago.date():
        start_hist = oclock_hour.date()
        end_hist = oclock_hour + timedelta(days=1)
        end_hist = end_hist.date()
        data_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&" \
                   f"start_date={start_hist}&end_date={end_hist}&hourly=temperature_2m," \
                   "relativehumidity_2m,dewpoint_2m,precipitation,snowfall,pressure_msl,windspeed_10m," \
                   "winddirection_10m,direct_radiation,diffuse_radiation,direct_normal_irradiance&windspeed_unit=ms" \
                   "&timezone=auto"
    else:
        data_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&past_days=10&hourly=temperature_2m," \
                   "relativehumidity_2m,dewpoint_2m,precipitation,snowfall,pressure_msl,windspeed_10m,winddirection_10m," \
                   "direct_radiation,diffuse_radiation,direct_normal_irradiance&windspeed_unit=ms&timezone=auto"
     
    res = requests.get(data_url).json()
     
    timestamps_str = res["hourly"]["time"]    
    temperature = res["hourly"]["temperature_2m"]
    humidity = res["hourly"]["relativehumidity_2m"]
    dew = res["hourly"]["dewpoint_2m"]
    precip = res["hourly"]["precipitation"]    
    snow = [i / 100 for i in res["hourly"]["snowfall"]]  # conversion de cm a m
    press = [j * 100 for j in res["hourly"]["pressure_msl"]]   # conversion de hPa a Pa
    windspeed = res["hourly"]["windspeed_10m"]
    winddir = res["hourly"]["winddirection_10m"]
    direct = res["hourly"]["direct_radiation"]
    diffuse = res["hourly"]["diffuse_radiation"]
    dni = res["hourly"]["direct_normal_irradiance"]
    tz = pytz.timezone(res["timezone"])
    timestamps = [datetime.strptime(time, "%Y-%m-%dT%H:%M") for time in timestamps_str]
    timestamps_loc = [tz.localize(time) for time in timestamps]
    zipped = list(zip(timestamps_loc, temperature, humidity, dew, precip, snow, press,
                      windspeed, winddir, direct, diffuse, dni))
    registry_df = pd.DataFrame(data=zipped,
                               columns=["datetime", "Amb_Temp", "Humidity", "Dew", "Rain_Acc",
                                        "Snow", "Atm_Press", "Wind_Sp", "Wind_Dir", "Irr_H",
                                        "Irr_Diffuse", "Irr_POA"])

    variables = ['Amb_Temp', 'Humidity', 'Dew', 'Snow', 'Atm_Press', 'Wind_Sp',
                 'Wind_Dir', 'Irr_H', 'Irr_Diffuse', 'Irr_POA']
    
    query_rain = f"""SELECT distinct signal_readi from signals s
                    where signal_readi_complete ~ 'WS_OM.Rain'
                    and signal_readi_complete ~ '^({id_planta})';
                    """
    response = fetch_data(query_rain)
    rain_type = list(response['signal_readi'])
    rain_var = 'Rain' if rain_type == [] else rain_type[0]  
    variables.append(rain_var)
    registry_df.rename(columns={'Rain_Acc': rain_var}, inplace=True)
      
    query = f"""SELECT distinct id, signal_readi_complete
                FROM public.signals 
                WHERE signal_readi_complete ~ '^({id_planta}.WS_OM)';"""  
    df_id_data = fetch_data(query) 

    for var in variables:     
        print("\n\n\n")       
        print(var)
        signal_id = df_id_data[df_id_data["signal_readi_complete"] ==\
                              f'{id_planta}.WS_OM.{var}']['id'].iloc[0]              
        df = registry_df[['datetime', var]]
        df.columns = ['ts', 'value']

        if freq == '5M':            
            df_new = constant_fiveminutal(df) if var ==\
                  'Rain' else fiveminutal(df)           
        elif freq == '10M':
            df_new = constant_tenminutal(df) if var ==\
                'Rain' else tenminutal(df)           

        df_new['signal'] = signal_id
        
        start_hour = oclock_hour.replace(hour=0, minute=5)        
        if oclock_hour.date() < tendays_ago.date():
            final_hour = oclock_hour.replace(hour=23,
                                             minute=55,
                                             second=0, microsecond=0)            
        else:
            start_hour = start_hour - timedelta(days=1)
            final_hour = start_hour.replace(hour=23, minute=55)
            final_hour = final_hour + timedelta(days=6)

        sub_df = df_new[(df_new["ts"] >= start_hour)\
                         & (df_new["ts"] <= final_hour)]
        
        if 'Rain' in var:
            sub_df = accum_to_instant(sub_df, var, freq, start_hour)           
        
        insert_df_to_database(sub_df,
                              'tmp_diego_curtailmentprediction1hour')
    return sub_df


def fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq('5T').interpolate('linear')
    return df.reset_index()


def tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T').interpolate(method='linear')
    return df.reset_index()


def constant_fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq('5T').interpolate('linear')
    return df.reset_index()


def constant_tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T', method='ffill')
    df = df.reset_index()
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    return df


def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    instant_df = acc_df.copy()
    # Dividir 'value' según la frecuencia
    freq_divisors = {'5M': 12, '10M': 6}
    instant_df['value'] /= freq_divisors[freq]

    if rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now,
                                      periods=9, freq='D')
        instant_df = pd.concat([
            instant_df[instant_df['ts']
                       .between(start, end, inclusive='left')]
                       .assign(value=lambda df: df['value'].cumsum())
            for start, end in zip(forecast_days[:-1], forecast_days[1:])
        ], ignore_index=True)

    return instant_df
