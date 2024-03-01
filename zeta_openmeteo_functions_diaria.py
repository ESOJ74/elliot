import logging
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests

from commons.SQLConnections import fetch_data, insert_df_to_database
from zeta_func_aux import (
    accum_to_instant,
    constant_fiveminutal,
    constant_tenminutal,
    fiveminutal,
    tenminutal,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def check_meteo_signals(id_plant, lon, lat, provider):

    # Chequear si existe el asset de la virtual weather station de la planta
    query_asset = (
        f"SELECT asset_id FROM assets WHERE readi_id = '{id_plant}.WS_{provider}';"
    )
    try:
        df = fetch_data(query_asset).values[0][0]
        print(df)
    except (
        IndexError,
        TypeError,
    ):  # Insertar el nuevo asset de la virtual weather station
        insert_asset_query = f"""INSERT INTO 
                                  public.assets(readi_id, pf_id, pf_name, "type",
                                  "meterType", scada_opc, scada_name, layout_name,
                                  field_name, readi_table_name, readi_layout_name,
                                  readi_parent_id, latitude, longitude, parent_id,
                                  field_id, plant, second_parent, third_parent,
                                  popup_text, gpoa_on_tracker, cell_on_tracker,
                                  type_idare, tracker_layout_as_built_name,
                                  mtrk_parent, manufacturer, model, asset_type) 
                                  VALUES('{id_plant}.WS_{provider}','','','Virtual_WeatherStation',
                                  '','','','','','','','{id_plant}',{lat},{lon},'','','{id_plant}',
                                  '','','','','','Virtual_Weather_Station_{provider}','','','','',
                                  'Virtual');"""
        # insert_data(insert_asset_query, f"DAG-{provider}-checkmeteosignals")

    # Chequear si existen senales para el asset_id
    query_signals = f"""SELECT id
                        FROM public.signals
                        WHERE signal_readi_complete ~ '{id_plant}.WS_{provider}';"""
    try:
        df = fetch_data(query_signals).values[0][0]
        print(df)
    except (
        IndexError,
        TypeError,
    ):  # Insertar las senales de la virtual weather station
        vars_dict = {
            "Amb_Temp": [
                "Instantaneous",
                "°C",
                "Temperature",
                "rd_vws_01",
                "Ambient Temperature (°C)",
                "Ambient Temperature - Amb_Temp (°C)",
                "Virtual Weather Station",
            ],
            "Atm_Press": [
                "Instantaneous",
                "Pa",
                "Sea level pressure",
                "rd_vws_02",
                "Atmospheric Pressure (Pa)",
                "Atmospheric Pressure - Atm_Press (Pa)",
                "Virtual Weather Station",
            ],
            "Dew": [
                "Instantaneous",
                "°C",
                "Dew point",
                "rd_vws_03",
                "Dew (°C)",
                "Dew Point - Dew (°C)",
                "Virtual Weather Station",
            ],
            "Humidity": [
                "Instantaneous",
                "%",
                "Relative humidity",
                "rd_vws_04",
                "Humidity (%)",
                "Humidity - Humidity (%)",
                "Virtual Weather Station",
            ],
            "Irr_Diffuse": [
                "Instantaneous",
                "W/m²",
                "Diffuse horizontal solar irradiance",
                "rd_vws_05",
                "Diffuse Irradiance (W/m²)",
                "Diffuse Irradiance - Irr_Diffuse (W/m²)",
                "Virtual Weather Station",
            ],
            "Irr_H": [
                "Instantaneous",
                "W/m²",
                "Global horizontal solar irradiance",
                "rd_vws_06",
                "Horizontal Irradiance (W/m²)",
                "Horizontal Irradiance - Irr_H (W/m²)",
                "Virtual Weather Station",
            ],
            "Irr_POA": [
                "Instantaneous",
                "W/m²",
                "Direct normal solar irradiance",
                "rd_vws_07",
                "Plane of Array Irradiance (W/m²)",
                "Plane of Array Irradiance - Irr_POA (W/m²)",
                "Virtual Weather Station",
            ],
            "Snow": [
                "Accumulated",
                "m",
                "Accumulated snowfall",
                "rd_vws_09",
                "Snow Accumulated (m/h)",
                "Snow Accumulated - Snow (m/h)",
                "Virtual Weather Station",
            ],
            "Wind_Dir": [
                "Instantaneous",
                "°",
                "Wind speed at 10 meters above ground",
                "rd_vws_10",
                "Wind Direction (°)",
                "Wind Direction - Wind_Dir (°)",
                "Virtual Weather Station",
            ],
            "Wind_Sp": [
                "Instantaneous",
                "m/s",
                "Wind direction at 10 meters above ground",
                "rd_vws_11",
                "Wind Speed (m/s)",
                "Wind Speed - Wind_Sp (m/s)",
                "Virtual Weather Station",
            ],
            "Rain_Acc_Daily": [
                "Accumulated",
                "mm/d",
                "Accumulated liquid equivalent precipitation",
                "rd_vws_08",
                "Rain Accumulated Daily (mm/d)",
                "Rain Accumulated Daily - Rain_Acc_Daily (mm/d)",
                "Virtual Weather Station",
            ],
        }

        for k, v in vars_dict.items():
            insert_signal_query = f"""INSERT INTO 
                                  public.signals(asset_readi, signal_readi_complete,
                                  signal_readi, type_signal, unit, essential,
                                  signal_description, active, expected_data, source,
                                  kpi_used, used_for_voltage_alert, dashboard_group,
                                  dashboard_group_row_title, dashboard_group_panel_title,
                                  dashboard_group_panel_description) 
                                  VALUES('{id_plant}.WS_{provider}',
                                  '{id_plant}.WS_{provider}.{k}', '{k}', '{v[0]}',
                                  '{v[1]}', 0, '{v[2]}', True, True, 'API_METEO', False,
                                  False, '{v[3]}', '{v[4]}', '{v[5]}', '{v[6]}');"""
            # insert_data(insert_signal_query, f"DAG-{provider}-checkmeteosignals")

def captura_openmeteo_diaria(
    lat: float, lon: float, exec_hour: datetime, freq: str, id_planta: str
):

    oclock_hour = exec_hour.replace(
        minute=0, second=0, microsecond=0
    )  # dejar la hora solamente
    tendays_ago = datetime.now() + timedelta(days=-10)

    if oclock_hour.date() < tendays_ago.date():
        start_hist = oclock_hour.date()
        end_hist = oclock_hour + timedelta(days=1)
        end_hist = end_hist.date()
        data_url = (
            f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&"
            f"start_date={start_hist}&end_date={end_hist}&hourly=temperature_2m,"
            "relativehumidity_2m,dewpoint_2m,precipitation,snowfall,pressure_msl,windspeed_10m,"
            "winddirection_10m,direct_radiation,diffuse_radiation,direct_normal_irradiance&windspeed_unit=ms"
            "&timezone=auto"
        )
    else:
        data_url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&past_days=10&hourly=temperature_2m,"
            "relativehumidity_2m,dewpoint_2m,precipitation,snowfall,pressure_msl,windspeed_10m,winddirection_10m,"
            "direct_radiation,diffuse_radiation,direct_normal_irradiance&windspeed_unit=ms&timezone=auto"
        )

    res = requests.get(data_url).json()

    timestamps_str = res["hourly"]["time"]
    temperature = res["hourly"]["temperature_2m"]
    humidity = res["hourly"]["relativehumidity_2m"]
    dew = res["hourly"]["dewpoint_2m"]
    precip = res["hourly"]["precipitation"]
    snow = [i / 100 for i in res["hourly"]["snowfall"]]  # conversion de cm a m
    press = [j * 100 for j in res["hourly"]["pressure_msl"]]  # conversion de hPa a Pa
    windspeed = res["hourly"]["windspeed_10m"]
    winddir = res["hourly"]["winddirection_10m"]
    direct = res["hourly"]["direct_radiation"]
    diffuse = res["hourly"]["diffuse_radiation"]
    dni = res["hourly"]["direct_normal_irradiance"]
    tz = pytz.timezone(res["timezone"])
    timestamps = [datetime.strptime(time, "%Y-%m-%dT%H:%M") for time in timestamps_str]
    timestamps_loc = [tz.localize(time) for time in timestamps]
    zipped = list(
        zip(
            timestamps_loc,
            temperature,
            humidity,
            dew,
            precip,
            snow,
            press,
            windspeed,
            winddir,
            direct,
            diffuse,
            dni,
        )
    )
    registry_df = pd.DataFrame(
        data=zipped,
        columns=[
            "datetime",
            "Amb_Temp",
            "Humidity",
            "Dew",
            "Rain_Acc",
            "Snow",
            "Atm_Press",
            "Wind_Sp",
            "Wind_Dir",
            "Irr_H",
            "Irr_Diffuse",
            "Irr_POA",
        ],
    )   

    query_rain = f"""SELECT distinct signal_readi from signals s
                    where signal_readi_complete ~ 'WS_OM.Rain'
                    and signal_readi_complete ~ '^({id_planta})';
                    """
    response = fetch_data(query_rain)
    rain_type = list(response["signal_readi"])
    rain_var = rain_type[0] if rain_type else "Rain"
    variables = [
        "Amb_Temp",
        "Humidity",
        "Dew",
        "Snow",
        "Atm_Press",
        "Wind_Sp",
        "Wind_Dir",
        "Irr_H",
        "Irr_Diffuse",
        "Irr_POA",
        rain_var
    ]   
    registry_df.rename(columns={"Rain_Acc": rain_var}, inplace=True)

    query = f"""SELECT distinct id, signal_readi_complete
                FROM public.signals 
                WHERE signal_readi_complete ~ '^({id_planta}.WS_OM)';"""
    df_id_data = fetch_data(query)

    for var in variables:
        signal_id = df_id_data[
            df_id_data["signal_readi_complete"] == f"{id_planta}.WS_OM.{var}"
        ]["id"].iloc[0]
        df = registry_df[["datetime", var]]
        df.columns = ["ts", "value"]

        if freq == "5M":
            df_new = constant_fiveminutal(df) if var == "Rain" else fiveminutal(df)
        elif freq == "10M":
            df_new = constant_tenminutal(df) if var == "Rain" else tenminutal(df)

        df_new["signal"] = signal_id

        start_hour = oclock_hour.replace(hour=0, minute=5)
        if oclock_hour.date() < tendays_ago.date():
            final_hour = oclock_hour.replace(
                hour=23, minute=55, second=0, microsecond=0
            )
        else:
            start_hour = start_hour - timedelta(days=1)
            final_hour = start_hour.replace(hour=23, minute=55)
            final_hour = final_hour + timedelta(days=6)

        sub_df = df_new[(df_new["ts"] >= start_hour) & (df_new["ts"] <= final_hour)]

        if "Rain" in var:
            sub_df = accum_to_instant(sub_df, var, freq, start_hour)

        insert_df_to_database(sub_df, "tmp_diego_curtailmentprediction1hour")
    return sub_df
