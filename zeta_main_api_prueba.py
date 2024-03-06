from datetime import datetime

import pytz

from commons.SQLConnections import fetch_data
from zeta_openmeteo_functions_diaria import (captura_openmeteo_diaria,
                                             check_meteo_signals)
from weatherbit_functions import captura_weatherbit
from commons.SQLConnections import connections

__TZ_HOST__ = "UTC"

def get_params(plant_id):    
     
    query = f"""SELECT readi_id, latitude, longitude, zona_horaria
                FROM public.plants
                WHERE readi_id='{plant_id}';"""
    plant_data = fetch_data(query)
    zonah = plant_data["zona_horaria"][0]
    lat = plant_data["latitude"][0]
    lon = plant_data["longitude"][0]    
    return lat, lon, zonah



id_planta = "01.01.01.001"
date_str = "2024-03-06T00:05:00+00:00".split("+")[0]
#date_str = "2024-02-21T05:05:00+00:00".split("+")[0]
exec_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
now_host = pytz.timezone(__TZ_HOST__).localize(exec_date)
lat, lon, zonah = get_params(id_planta)
tz = pytz.timezone(zonah)
exec_hour = now_host.astimezone(tz)

#check_meteo_signals(id_planta, lon, lat, "OM")
#captura_openmeteo_diaria(lat, lon, exec_hour, "10M", id_planta)
captura_weatherbit(lat, lon, id_planta, zonah, exec_hour, "10M")
#captura_openmeteo_diaria(lat, lon, exec_hour, "10M", id_planta)

"""cursor = connections().cursor()
table ='public.tmp_diego_rawdata_pruebasupsert '
query = f"SELECT * FROM {table} order by ts desc limit 5"
cursor.execute(query)
result = cursor.fetchall()
cursor.close()
print(result)"""
#df = pd.read_sql(query, conn)
#print(df)
