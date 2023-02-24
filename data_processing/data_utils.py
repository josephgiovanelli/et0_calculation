# Headers
VALUE_TYPES = [
    ["ground_potential", "GRND_WATER_G"],
    ["meteo", "WIND_GUST_MAX"],
    ["meteo", "SOLAR_RAD"],
    ["meteo", "BATTERY_TENS"],
    ["meteo", "PLUV_CURR"],
    ["meteo", "AIR_TEMP"],
    ["ground_potential", "DRIPPER"],
    ["meteo", "AIR_HUM"],
    ["meteo", "WIND_SPEED"],
    ["ground_potential", "BATTERY_TENS"],
    ["meteo", "ETC"],
    ["meteo", "WIND_DIRECTION"],
    ["meteo", "ET0"],
]
VALUE_TYPES_INDEXES = [
    "wc",
    "max_wind",
    "solar_radiation",
    "bat_tens",
    "precipitation",
    "air_temperature",
    "irrigation",
    "air_humidity",
    "wind_speed",
    "bt",
    "etc",
    "wind_dir",
    "et0",
]
USER_IN_PLANT = [
    "source",
    "RefStructureId" "refStructureName",
    "companyName",
    "fieldName",
    "plantNum",
    "plantRow",
    "userId",
    "wateringAdvice",
]
VIEW_DATA_ORIGINAL = [
    "source",
    "refStructureId",
    "refStructureName",
    "companyId",
    "companyName",
    "fieldId",
    "fieldName",
    "plantId",
    "plantName",
    "plantNum",
    "plantRow",
    "colture",
    "coltureType",
    "nodeId",
    "nodeDescription",
    "detectedValueTypeId",
    "detectedValueTypeDescription",
    "yy",
    "xx",
    "value",
    "unit",
    "date",
    "time",
    "latitude",
    "longitude",
    "timestamp",
    "zz",
]
TRANSCODING_FIELD = [
    "source",
    "refStructureId",
    "companyId",
    "fieldId",
    "plantId",
    "nodeId",
    "refStructureName",
    "companyName",
    "fieldName",
    "plantNum",
    "plantRow",
    "colture",
    "coltureType",
    "parcelCode",
    "address",
    "refNode",
    "doProfile",
    "xxProfile",
    "yyProfile",
    "zzProfile",
    "sensorsNumber",
]
HUMIDITY_BIN = [
    "timestamp",
    "umidity_bin",
    "count",
    "refStructureName",
    "companyName",
    "fieldName",
    "plantNum",
    "plantRow",
    "dumpId",
]
INTERPOLATED = [
    "xx",
    "yy",
    "value",
    "refStructureName",
    "companyName",
    "fieldName",
    "plantNum",
    "plantRow",
    "timestamp",
    "dumpId",
    "zz",
]

# Simulation params
HUM_BINS = [0, 30, 100, 300, 1500, 10000]

# Field params
LATITUDE = 44.23484
LONGITUDE = 11.8016815
PLANT_NUM = 1
PLANT_ROW = "T1 basso"
SOURCE = "iFarming"
USER_ID = 11
WATERING_ADVICE = False
STRUCTURE_ID = "ZESPRI"
COMPANY_ID = 227
FIELD_ID = 581
PLANT_ID = 1509
COLTURE = "Kiwi"
COLTURE_TYPE = "G3"
XXPROFILE = 100
YY_PROFILE = 0
ZZPROFILE = -60
SENSORS_NUMBER = 12
PLANT_NAME = ""
NODE_DESCRIPTION = ""
PARCEL_CODE = ""
ADDRESS = ""
REF_NODE = ""
REF_STRUCTURE_NAME = "-"
COMPANY_NAME = "EVALUATION"
FIELD_NAME = "OBS, i.e., no"

# uploader params
INITIAL_STATE = REF_STRUCTURE_NAME
COST_MATRIX = COMPANY_NAME
APPROACH = FIELD_NAME

# Data granularity
X = ["z20", "z40", "z60"]
Z = ["x0", "x25", "x50", "x80"]


def get_value_type(value_type):
    return VALUE_TYPES[VALUE_TYPES_INDEXES.index(value_type)][0]


def get_value_type_id(value_type):
    return VALUE_TYPES[VALUE_TYPES_INDEXES.index(value_type)][1]
