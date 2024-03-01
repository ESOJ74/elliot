import psycopg2 as pg
import logging
import yaml
from pathlib import Path

logger = logging.getLogger()

try:
    __HOME__ = Path(r'C:\Users\joseluis.cuenca\OneDrive - Bosonit\Escritorio\Elliot\pruebas')
    __TZ_HOST__ = 'Europe/Madrid'
except:
    __HOME__ = Path('/opt/meteorology')
    __TZ_HOST__ = 'UTC'

"""__HOME__ = Path('/opt/meteorology')
__TZ_HOST__ = 'UTC'"""


def connections():
    with open(__HOME__ / 'opdenergy.yaml', 'r') as handler:
        config = yaml.safe_load(handler)
    
    params_dic = {
        "host"      : config['bbdd_params']['host'],
        "database"  : config['bbdd_params']['database'],
        "user"      : config['bbdd_params']['user'],
        "password"  : config['bbdd_params']['password'],
        "port"      : config['bbdd_params']['port'],
        "application_name": config['bbdd_params']['application_name']
    }
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = pg.connect(**params_dic)
    except (Exception, pg.DatabaseError) as e:
        logger.info(e)    
    return conn