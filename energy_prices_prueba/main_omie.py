import os.path
import requests
import urllib.request
import pathlib
import json
import pandas as pd
import logging
import pytz
from datetime import datetime, timedelta
from commons.SQLConnections import fetch_data, insert_df_to_database
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def get_raw_omie(fecha, id_omie, zonah):

    fecha = fecha.split('T')[0].replace('-', '')
    file_url = f"https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbc&filename=marginalpdbc_{fecha}.1"
    req = requests.get(file_url)
    file_location = 'last_omie_file.csv'
    urllib.request.urlretrieve(req.url, file_location)

    with open(file_location) as f:
        contenido = [line.rstrip() for line in f]
    contenido = contenido[1:-1]
    
    if contenido == []:
        logger.info("El fichero no está disponible")
        raise SystemExit(-1)

    precios_hrs = [item.split(';') for item in contenido] 
    df = pd.DataFrame(data=precios_hrs, columns=["Year", "Month", "Day", "Hour", "Port_price", "Spain_price", "Empty"])
    df.drop('Port_price', axis=1, inplace=True)
    df.drop('Empty', axis=1, inplace=True)

    df2 = pd.to_datetime(df[["Year", "Month", "Day", "Hour"]])

    df3 = pd.DataFrame(df2, columns=["ts"])
    df3.ts = df3.ts.dt.tz_localize(zonah, nonexistent='shift_forward') #.dt.tz_convert('Europe/Madrid')
    df3["calc_sig_id"] = id_omie
    assert len(df3) == len(df)
    df3["value"] = df["Spain_price"].astype(float)
    df3["value"] = df3["value"] / 1000  # el precio debe ser en KWh no en MWh 
    return df3


def fivemin_inter(df, timezone):
    comienzo = df["ts"][0]
    final = df["ts"][len(df)-1]
    final = final.replace(minute=55)
    fivemin_reg = pd.date_range(start=comienzo,
                                end=final, inclusive='both', freq='5T')
    return pd.DataFrame({'ts': fivemin_reg})


def price_register(df_omie, timezone, calc_sig_id):
    df = fivemin_inter(df_omie, timezone)
    df["calc_sig_id"] = calc_sig_id
    df["value"] = 0
    for i in range(len(df_omie)):
        df.loc[df["ts"] >= df_omie["ts"][i], ["value"]] = df_omie["value"][i]
    return df


def process_omie(start_date, end_date): 
    # Query para recuperar la calc_sig_id de cada país (España, Chile) para la tabla de registros y la 5minutal
    query_signals = """SELECT calc_sig_id, timezone FROM public.energyprice_calculated_signals WHERE provider = 'OMIE';"""    
    result = fetch_data(query_signals)
    calc_sig_id = result["calc_sig_id"][0]
    zonah = result["timezone"][0]
    df_raw = get_raw_omie(end_date, calc_sig_id, zonah)
    df_raw = df_raw.rename(columns={'calc_sig_id': 'signal'})    
    insert_df_to_database(df_raw, 'tmp_diego_rawdata_pruebasupsert') #'energyprice_raw_data'

    price_minutal = price_register(df_raw, zonah, calc_sig_id)
    price_minutal = price_minutal.rename(columns={'calc_sig_id': 'signal'})
    insert_df_to_database(price_minutal, 'tmp_diego_rawdata_pruebasupsert') #'energyprice_calculated_signals_data'
