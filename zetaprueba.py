

from zetafunciones import check_meteo_signals, captura_weatherbit, fetch_data
import pytz
from datetime import datetime
from commons.SQLConnections import fetch_data   


__TZ_HOST__ = 'UTC'

date_str = "2024-02-22T10:05:00"    
date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")  
freq = "5M"
plant = "01.01.01.004"

query = f"""SELECT readi_id, latitude, longitude, zona_horaria FROM public.plants WHERE readi_id='{plant}';"""
plant_data = fetch_data(query)
zonah = plant_data['zona_horaria'][0]
lat = plant_data['latitude'][0]
lon = plant_data['longitude'][0]
id_planta = plant_data['readi_id'][0]

now_host = pytz.timezone(__TZ_HOST__).localize(date)
tz = pytz.timezone(zonah)  # Europe/Madrid o America/Santiago, depende de la planta
exec_hour = now_host.astimezone(tz)



#check_meteo_signals(id_planta, lon, lat, 'WB')


captura_weatherbit(lat, lon, id_planta, zonah, exec_hour, freq)







