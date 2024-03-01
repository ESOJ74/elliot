import os
from datetime import datetime
from commons.SQLConnections import fetch_data, insert_data
from openmeteo_functions_diaria import captura_openmeteo_diaria
from fastapi import FastAPI, Request
import uvicorn
import pytz

__TZ_HOST__ = 'UTC'

app = FastAPI()


def check_meteo_signals(id_plant, lon, lat, provider):
      
      # Chequear si existe el asset de la virtual weather station de la planta
      query_asset = f"SELECT asset_id FROM assets WHERE readi_id = '{id_plant}.WS_{provider}';"
      try:
          df = fetch_data(query_asset).values[0][0]
          print(df)
      except (IndexError, TypeError):  # Insertar el nuevo asset de la virtual weather station           
          insert_asset_query = f"""INSERT INTO 
                                  public.assets(readi_id, pf_id, pf_name, "type", "meterType", scada_opc,
                                  scada_name, layout_name, field_name, readi_table_name, readi_layout_name,
                                  readi_parent_id, latitude, longitude, parent_id, field_id, plant,
                                  second_parent, third_parent, popup_text, gpoa_on_tracker,
                                  cell_on_tracker, type_idare, tracker_layout_as_built_name,
                                  mtrk_parent, manufacturer, model, asset_type) 
                                  VALUES('{id_plant}.WS_{provider}','','','Virtual_WeatherStation',
                                  '','','','','','','','{id_plant}',{lat},{lon},'','','{id_plant}',
                                  '','','','','','Virtual_Weather_Station_{provider}','','','','',
                                  'Virtual');"""
          #insert_data(insert_asset_query, f"DAG-{provider}-checkmeteosignals") 

      # Chequear si existen senales para el asset_id
      query_signals = f"SELECT id FROM public.signals WHERE signal_readi_complete ~ '{id_plant}.WS_{provider}';"
      try:
          df = fetch_data(query_signals).values[0][0]   
          print(df)       
      except (IndexError, TypeError):  # Insertar las senales de la virtual weather station
          vars_dict = {
              'Amb_Temp': ['Instantaneous', '°C', 'Temperature', 'rd_vws_01', 'Ambient Temperature (°C)', 'Ambient Temperature - Amb_Temp (°C)', 'Virtual Weather Station'],
              'Atm_Press': ['Instantaneous', 'Pa', 'Sea level pressure', 'rd_vws_02', 'Atmospheric Pressure (Pa)', 'Atmospheric Pressure - Atm_Press (Pa)', 'Virtual Weather Station'],
              'Dew': ['Instantaneous', '°C', 'Dew point', 'rd_vws_03', 'Dew (°C)', 'Dew Point - Dew (°C)', 'Virtual Weather Station'],
              'Humidity':['Instantaneous', '%', 'Relative humidity', 'rd_vws_04', 'Humidity (%)', 'Humidity - Humidity (%)', 'Virtual Weather Station'],
              'Irr_Diffuse': ['Instantaneous', 'W/m²', 'Diffuse horizontal solar irradiance', 'rd_vws_05', 'Diffuse Irradiance (W/m²)', 'Diffuse Irradiance - Irr_Diffuse (W/m²)', 'Virtual Weather Station'],
              'Irr_H': ['Instantaneous', 'W/m²', 'Global horizontal solar irradiance', 'rd_vws_06', 'Horizontal Irradiance (W/m²)', 'Horizontal Irradiance - Irr_H (W/m²)', 'Virtual Weather Station'],
              'Irr_POA': ['Instantaneous', 'W/m²', 'Direct normal solar irradiance', 'rd_vws_07', 'Plane of Array Irradiance (W/m²)', 'Plane of Array Irradiance - Irr_POA (W/m²)', 'Virtual Weather Station'],
              'Snow': ['Accumulated', 'm', 'Accumulated snowfall', 'rd_vws_09', 'Snow Accumulated (m/h)', 'Snow Accumulated - Snow (m/h)', 'Virtual Weather Station'],
              'Wind_Dir': ['Instantaneous', '°', 'Wind speed at 10 meters above ground', 'rd_vws_10', 'Wind Direction (°)', 'Wind Direction - Wind_Dir (°)', 'Virtual Weather Station'],
              'Wind_Sp': ['Instantaneous', 'm/s', 'Wind direction at 10 meters above ground', 'rd_vws_11', 'Wind Speed (m/s)', 'Wind Speed - Wind_Sp (m/s)', 'Virtual Weather Station'],
              'Rain_Acc_Daily': ['Accumulated','mm/d', 'Accumulated liquid equivalent precipitation', 'rd_vws_08', 'Rain Accumulated Daily (mm/d)',	'Rain Accumulated Daily - Rain_Acc_Daily (mm/d)', 'Virtual Weather Station']
          }

          for k, v in vars_dict.items():
              insert_signal_query = f"""INSERT INTO 
                                  public.signals(asset_readi, signal_readi_complete, signal_readi, type_signal, unit, essential, 
                                  signal_description, active, expected_data, source, kpi_used, used_for_voltage_alert,
                                  dashboard_group, dashboard_group_row_title, dashboard_group_panel_title, dashboard_group_panel_description) 
                                  VALUES('{id_plant}.WS_{provider}', '{id_plant}.WS_{provider}.{k}', '{k}', '{v[0]}', '{v[1]}', 0,
                                  '{v[2]}', True, True, 'API_METEO', False, False,
                                  '{v[3]}', '{v[4]}', '{v[5]}', '{v[6]}');
                                  """
              #insert_data(insert_signal_query, f"DAG-{provider}-checkmeteosignals") 


def get_params():
    date_str = "2024-02-29T12:00:00+00:00".split('+')[0] 
    exec_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    plant = "01.01.01.001"

    query = f"""SELECT readi_id, latitude, longitude, zona_horaria FROM public.plants WHERE readi_id='{plant}';"""
    plant_data = fetch_data(query)
    zonah = plant_data['zona_horaria'][0]
    lat = plant_data['latitude'][0]
    lon = plant_data['longitude'][0]
    id_planta = plant_data['readi_id'][0]
    return id_planta, lat, lon, exec_date, id_planta, zonah




id_planta, lat, lon, exec_date, id_planta, zonah = get_params()
print(id_planta, lat, lon, exec_date, id_planta, zonah)
#check_sun_signals(id_planta, lon, lat, 'SS')
now_host = pytz.timezone(__TZ_HOST__).localize(exec_date)
tz = pytz.timezone(zonah)  # Europe/Madrid o America/Santiago, depende de la planta
exec_hour = now_host.astimezone(tz)
print(exec_hour)

check_meteo_signals(id_planta, lon, lat, 'OM')
captura_openmeteo_diaria(lat, lon, exec_hour, '5M', id_planta)


"""36583
147839"""

