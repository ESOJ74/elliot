import platform
import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, Request
import uvicorn

from main_omie import process_omie
from main_igx import process_igx


if platform.system() == 'Windows':
    __HOME__ = Path(r'C:\Users\julia.sanchez\OneDrive - Bosonit\repo\energy-prices')
    __TZ_HOST__ = 'Europe/Madrid'
elif platform.system() == 'Linux':
    __HOME__ = Path('/opt/energy-prices')
    __TZ_HOST__ = 'UTC'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


app = FastAPI()

@app.get('/omieespana')
async def omieprices(request: Request) -> str:   
    start_date_str = request.query_params.get('start_date')
    end_date_str = request.query_params.get('end_date')
    process_omie(start_date_str, end_date_str)
    return "ok"


@app.get('/igxchile')
async def igxprices(request: Request) -> str:    
    start_date_str = request.query_params.get('start_date')
    end_date_str = request.query_params.get('end_date')

    princ_mes = start_date_str.split('T')[0]
    ts_comienzo = datetime.strptime(princ_mes, "%Y-%m-%d")
    fin_mes = end_date_str.split('T')[0]
    ts_final = datetime.strptime(fin_mes, "%Y-%m-%d")
    ts_final = ts_final.replace(hour=23, minute=55)

    process_igx(princ_mes, fin_mes, ts_comienzo)

    return "ok"

start_date_str = '2023-12-01T00:01:00+00:00'
end_date_str = '2024-01-31T00:01:00+00:00'
#process_omie(start_date_str, end_date_str)

princ_mes = start_date_str.split('T')[0]
ts_comienzo = datetime.strptime(princ_mes, "%Y-%m-%d")
fin_mes = end_date_str.split('T')[0]
plantas = ['SOL_DE_LOS_ANDES', 'LA_ESTRELLA', 'LLAY_LLAY', 'PMGD_LINGUE']
for planta in plantas:
    process_igx(princ_mes, fin_mes, ts_comienzo, planta)
