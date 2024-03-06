import requests
import pytz
import logging
import pandas as pd
import re
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from commons.SQLConnections import fetch_data, insert_df_to_database


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def metadatos_plantas():
    query_datos = """SELECT calc_sig_id, region, coordinado_code, central_code, timezone
                     FROM public.energyprice_calculated_signals WHERE country='Chile';"""
    result = fetch_data(query_datos).set_index('calc_sig_id').to_dict('index')

    coordcode_data = {k: v['coordinado_code'] for k, v in result.items()}
    centralcode_data = {k: v['central_code'] for k, v in result.items()}
    time_data = {k: v['timezone'] for k, v in result.items()}

    return coordcode_data, centralcode_data, time_data


def get_raw_igx(princ_mes, fin_mes, id_igx, coord_code, planta, zonah):
    headers = {
        'X-Api-Key': '2mIhHBKi993811k-3CsJZdzc-2_mFGZn:cmarquez@opdenergy.com'
              }

    file_url = f'https://app.igx.cl/api/download-center/export?coordinado_code={coord_code}&central_code={planta}&from_date={princ_mes}&to_date={fin_mes}&export_type=balanceCmg'
    req = requests.get(file_url, headers=headers)
    
    contenido = req.text.split('\r\n')[1:]
    precios_hrs = [[re.sub(r'"', '', x) for x in item.split(',')] for item in contenido]

    df = pd.DataFrame(data=precios_hrs, columns=["Central", "Codigo", "Barra", "Timezone", "ts", "Precio_CLP", "Precio_USD"])
    df = df.drop(['Precio_CLP', 'Timezone', 'Codigo', 'Barra', 'Central'], axis=1)

    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(pytz.timezone(zonah), ambiguous='infer', nonexistent='shift_forward')

    df["calc_sig_id"] = id_igx
    df["value"] = df["Precio_USD"].astype(float) / 1000  # Convert to float and change unit to kWh
    df = df.drop('Precio_USD', axis=1)
    return df


def fivemin_inter(df, timezone):
    comienzo = df["ts"].iloc[0]
    final = df["ts"].iloc[-1]
    final = final.replace(minute=55)
    fivemin_reg = pd.date_range(start=comienzo,
                                end=final, inclusive='both', freq='5T')
    df = pd.DataFrame({'ts': fivemin_reg})
    return df


def price_register(df_igx, timezone, calc_sig_id):
    df = fivemin_inter(df_igx, timezone)
    df["calc_sig_id"] = calc_sig_id
    for i in range(len(df_igx)):
        df.loc[df["ts"] >= df_igx["ts"][i], ["value"]] = df_igx["value"][i]
    return df


def null_register(comienzo, final, calc_sig_id):
    fivemin_reg = pd.date_range(start=comienzo,
                                end=final, inclusive='both', freq='5T')
    df = pd.DataFrame({'ts': fivemin_reg})
    df["calc_sig_id"] = calc_sig_id
    df["value"] = None
    return df


def process_igx(princ_mes, fin_mes, ts_comienzo):

    coordcode_data, centralcode_data, time_data = metadatos_plantas()    
    comienzo_nulls = ts_comienzo.replace(month=1 if ts_comienzo.month == 12 else ts_comienzo.month + 1,
                                         year=ts_comienzo.year + 1 if ts_comienzo.month == 12 else ts_comienzo.year
                                         )
    
    fin_nulls = ts_comienzo + relativedelta(months=3)
    fin_nulls = fin_nulls + timedelta(minutes=-5)

    for id_igx in coordcode_data.keys():
        timezone = time_data.get(id_igx)
        tz = pytz.timezone(timezone)
        coord_code = coordcode_data.get(id_igx)
        planta = centralcode_data.get(id_igx)

        df_raw = get_raw_igx(princ_mes, fin_mes, id_igx, coord_code, planta, timezone)
        price_minutal = price_register(df_raw, timezone, id_igx) 
        comienzo_nulls_loc = tz.localize(comienzo_nulls, is_dst=None)
        fin_nulls_loc = tz.localize(fin_nulls, is_dst=None)
        df_nulls = null_register(comienzo_nulls_loc, fin_nulls_loc, id_igx)

        df_raw = df_raw.rename(columns={'calc_sig_id': 'signal'})     
        #insert_df_to_database(df_raw, 'tmp_diego_rawdata_pruebasupsert') #'energyprice_raw_data'
        price_minutal = price_minutal.rename(columns={'calc_sig_id': 'signal'})      
        #insert_df_to_database(price_minutal, 'tmp_diego_rawdata_pruebasupsert')#'energyprice_calculated_signals_data'                   
        df_nulls = df_nulls.rename(columns={'calc_sig_id': 'signal'})        
        #insert_df_to_database(df_nulls, 'tmp_diego_rawdata_pruebasupsert') #'energyprice_calculated_signals_data'
        