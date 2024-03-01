from datetime import datetime

import pytz

from commons.SQLConnections import fetch_data
from zeta_openmeteo_functions_diaria import (captura_openmeteo_diaria,
                                             check_meteo_signals)

__TZ_HOST__ = "UTC"



def get_params():
    date_str = "2024-02-29T12:00:00+00:00".split("+")[0]
    exec_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    plant = "01.01.01.001"

    query = f"""SELECT readi_id, latitude, longitude, zona_horaria
                FROM public.plants
                WHERE readi_id='{plant}';"""
    plant_data = fetch_data(query)
    zonah = plant_data["zona_horaria"][0]
    lat = plant_data["latitude"][0]
    lon = plant_data["longitude"][0]
    id_planta = plant_data["readi_id"][0]
    return id_planta, lat, lon, exec_date, id_planta, zonah


id_planta, lat, lon, exec_date, id_planta, zonah = get_params()
print(id_planta, lat, lon, exec_date, id_planta, zonah)
# check_sun_signals(id_planta, lon, lat, 'SS')
now_host = pytz.timezone(__TZ_HOST__).localize(exec_date)
# Europe/Madrid o America/Santiago, depende de la planta
tz = pytz.timezone(zonah)
exec_hour = now_host.astimezone(tz)
print(exec_hour)

check_meteo_signals(id_planta, lon, lat, "OM")
captura_openmeteo_diaria(lat, lon, exec_hour, "5M", id_planta)
