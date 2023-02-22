import db_uploader
import data_utils as utils
import os 
from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect

SSH_USERNAME = ""
SSH_PSW = ""

INITIAL_STATE = "WET"
COST_MATRIX = "WITH"
APPROACH = "ET0 + DELTA"

DATA_PATH = "data"
METEO_PATH =  os.path.join(DATA_PATH,'meteo')
GROUND_PATH = os.path.join(DATA_PATH,"approaches",COST_MATRIX.lower()+"costmatrix",INITIAL_STATE.lower(),APPROACH.lower())

IRRIGATION_PATH = os.path.join(GROUND_PATH,"scheduledIrrigation.csv")
WC_PATH = os.path.join(GROUND_PATH,"outputWaterContent.csv")
WP_PATH = os.path.join(GROUND_PATH,"output.csv")
METEO_FILES = ['air_humidity.csv','air_temperature.csv','precipitation.csv','solar_radiation.csv','wind_speed.csv']

def setup_connection():
  server = SSHTunnelForwarder(('isi-alfa.csr.unibo.it', 22),
                              ssh_username= SSH_USERNAME,
                              ssh_password= SSH_PSW,
                              remote_bind_address=('137.204.74.52', 5432),
                              local_bind_address=('127.0.0.1', 23349))
  server.start()
  print("SSH OK")
  return connect(
      database='smart_irrigation',
      user='root',
      host=server.local_bind_host,
      port=server.local_bind_port,
      password='smart_irrigation')

conn = setup_connection()

#upload user_in_plant // first three parms are lists
db_uploader.user_in_plant(utils.INITIAL_STATE,utils.COST_MATRIX,utils.APPROACHES,conn)
#upload transcoding_field // first three parms are lists
db_uploader.transcoding_field(utils.INITIAL_STATE,utils.COST_MATRIX,utils.APPROACHES,conn)
#upload wc
db_uploader.sim_to_db(WC_PATH,INITIAL_STATE,COST_MATRIX,APPROACH,utils.get_value_type('wc'),utils.get_value_type_id('wc'),conn)
#upload irrigation
db_uploader.sim_to_db(IRRIGATION_PATH,INITIAL_STATE,COST_MATRIX,APPROACH,utils.get_value_type('irrigation'),utils.get_value_type_id('irrigation'),conn)
#upload humidity_bin
db_uploader.humidity_bin(WP_PATH,INITIAL_STATE,COST_MATRIX,APPROACH,conn)
#upload interpolated data
db_uploader.load_interpolated_data(WP_PATH,INITIAL_STATE,COST_MATRIX,APPROACH,conn)

#upload meteo
for i in range (0,len(METEO_FILES)):
    db_uploader.sim_to_db(os.path.join(METEO_PATH,METEO_FILES[i]),INITIAL_STATE,COST_MATRIX,APPROACH, utils.get_value_type(METEO_FILES[i].split('.')[0]),utils.get_value_type_id(METEO_FILES[i].split('.')[0]),conn)
conn.close()

