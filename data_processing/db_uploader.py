from datetime import datetime
import pandas as pd
import data_utils as utils
import psycopg2
import io


def prepare_dataset(path):
    temp = "(?:% s)" % "|".join(utils.X)
    temp2 = "(?:% s)" % "|".join(utils.Z)
    df = pd.read_csv(path)
    df = pd.DataFrame(df["timestamp"]).join(df.filter(regex=temp).filter(regex=temp2))
    outDF = pd.DataFrame(columns=["timestamp", "variable", "value"])

    melted = df.melt(
        col_level=0,
        id_vars=["timestamp"],
        value_vars=[x for x in df.columns if x != "timestamp"],
    )
    for v in melted["variable"].unique():
        f = melted[melted["variable"] == v]
        outDF = outDF.append(f, ignore_index=True)
    return outDF


def get_params(sensor_id, valueTypeId, conn):
    if isinstance(sensor_id, str):
        z = -int(sensor_id.split("_")[0].replace("z", ""))
        x = int(sensor_id.split("_")[2].replace("x", ""))
        conn.execute(
            """SELECT * FROM transcoding_sensor WHERE "detectedValueTypeId" = %s AND xx = %s AND yy = %s""",
            [
                valueTypeId,
                x,
                z,
            ],
        )
    else:
        conn.execute(
            """SELECT * FROM transcoding_sensor WHERE "detectedValueTypeId" = %s AND xx IS NULL AND yy IS NULL""",
            [
                valueTypeId,
            ],
        )

    row = conn.fetchone()
    unit = row[3]
    detectedValueTypeId = row[4]
    detectedValueType = row[5]
    z = row[6]
    x = row[7]

    return x, z, unit, detectedValueTypeId, detectedValueType


# Uploads WC or Irrigation
def sim_to_db(wc_path, valueType, valueTypeId, conn):
    print(
        "Uploading from "
        + str(wc_path.split("/")[len(wc_path.split("/")) - 1])
        + " "
        + str(utils.INITIAL_STATE)
        + " - "
        + str(utils.COST_MATRIX)
        + " cost matrix - "
        + str(utils.APPROACH)
    )
    if valueType == "meteo" or valueTypeId == "DRIPPER":
        dataset = pd.read_csv(wc_path)
        dataset = dataset.rename(
            columns={dataset.columns[0]: "variable", dataset.columns[1]: "value"}
        )
    else:
        dataset = prepare_dataset(wc_path)
    new_df = []
    dictionary = {}
    sql = conn.cursor()
    x = 0.0
    z = 0.0
    detectedValueTypeId = 0
    detectedValueDescription = 0
    for _, row in dataset.iterrows():
        if row["variable"] in dictionary:
            x = dictionary[row["variable"]][0]
            z = dictionary[row["variable"]][1]
            unit = dictionary[row["variable"]][2]
            detectedValueTypeId = dictionary[row["variable"]][3]
            detectedValueDescription = dictionary[row["variable"]][4]
        else:
            try:
                x, z, unit, detectedValueTypeId, detectedValueDescription = get_params(
                    row["variable"], valueTypeId, sql
                )
                dictionary[row["variable"]] = [
                    x,
                    z,
                    unit,
                    detectedValueTypeId,
                    detectedValueDescription,
                ]
            except Exception as error:
                print("Error while retrieving params: " + error)

        if valueType == "meteo" or valueTypeId == "DRIPPER":
            new_df.append(
                [
                    utils.SOURCE,
                    utils.STRUCTURE_ID,
                    utils.INITIAL_STATE,
                    utils.COMPANY_ID,
                    utils.COST_MATRIX,
                    utils.FIELD_ID,
                    utils.APPROACH,
                    utils.PLANT_ID,
                    utils.PLANT_NAME,
                    utils.PLANT_NUM,
                    utils.PLANT_ROW,
                    utils.COLTURE,
                    utils.COLTURE_TYPE,
                    valueType,
                    utils.NODE_DESCRIPTION,
                    detectedValueTypeId,
                    detectedValueDescription,
                    z,
                    x,
                    row["value"],
                    unit,
                    datetime.fromtimestamp(row["variable"]),
                    datetime.fromtimestamp(row["variable"]).strftime("%H:%M:%S"),
                    utils.LATITUDE,
                    utils.LONGITUDE,
                    int(row["variable"]),
                    None,
                ]
            )
        else:
            new_df.append(
                [
                    utils.SOURCE,
                    utils.STRUCTURE_ID,
                    utils.INITIAL_STATE,
                    utils.COMPANY_ID,
                    utils.COST_MATRIX,
                    utils.FIELD_ID,
                    utils.APPROACH,
                    utils.PLANT_ID,
                    utils.PLANT_NAME,
                    utils.PLANT_NUM,
                    utils.PLANT_ROW,
                    utils.COLTURE,
                    utils.COLTURE_TYPE,
                    valueType,
                    utils.NODE_DESCRIPTION,
                    detectedValueTypeId,
                    detectedValueDescription,
                    z,
                    x,
                    row["value"],
                    unit,
                    datetime.fromtimestamp(row["timestamp"]),
                    datetime.fromtimestamp(row["timestamp"]).strftime("%H:%M:%S"),
                    utils.LATITUDE,
                    utils.LONGITUDE,
                    int(row["timestamp"]),
                    None,
                ]
            )

    new_df = pd.DataFrame(new_df, columns=utils.VIEW_DATA_ORIGINAL)
    insert_with_string_io(new_df, "view_data_original", conn)
    print("Done")


# Uploads into transcoding_field
def transcoding_field(conn):
    print("Uploading transcoding_field datas...")
    nodeId = ["ground_potential", "meteo"]
    new_df = []
    for node_id in nodeId:
        new_df.append(
            [
                utils.SOURCE,
                utils.STRUCTURE_ID,
                utils.COMPANY_ID,
                utils.FIELD_ID,
                utils.PLANT_ID,
                node_id,
                utils.INITIAL_STATE,
                utils.COST_MATRIX,
                utils.APPROACH,
                utils.PLANT_NUM,
                utils.PLANT_ROW,
                utils.COLTURE,
                utils.COLTURE_TYPE,
                utils.PARCEL_CODE,
                utils.ADDRESS,
                utils.REF_NODE,
                True,
                utils.XXPROFILE,
                utils.YY_PROFILE,
                utils.ZZPROFILE,
                utils.SENSORS_NUMBER,
            ]
        )

    new_df = pd.DataFrame(new_df, columns=utils.TRANSCODING_FIELD)
    insert_with_string_io(new_df, "transcoding_field", conn)
    print("Done")
    return pd.DataFrame(new_df, columns=utils.TRANSCODING_FIELD)


# Uploads into humidity_bin
def humidity_bin(pot_path, conn):
    print(
        "Binning humidity from "
        + str(pot_path.split("/")[len(pot_path.split("/")) - 1])
        + " "
        + str(utils.INITIAL_STATE)
        + " - "
        + str(utils.COST_MATRIX)
        + " cost matrix - "
        + str(utils.APPROACH)
    )
    dataset = pd.read_csv(pot_path)
    HUM_BINS = [0, -30, -100, -300, -1500, -10000]
    new_df = []
    for _, row in dataset.iterrows():
        for i in range(0, len(HUM_BINS) - 1):
            count = 0
            for _, value in row.items():
                if abs(value) >= abs(HUM_BINS[i]) and abs(value) < abs(HUM_BINS[i + 1]):
                    count = count + 1
                    # If i'm checking for -300,-1500
                if (
                    HUM_BINS[i] == HUM_BINS[len(HUM_BINS) - 1]
                    and value >= HUM_BINS[len(HUM_BINS) - 1]
                ):
                    count = count + 1
            bin_str = "(" + str(HUM_BINS[i + 1]) + ", " + str(HUM_BINS[i]) + "]"
            new_df.append(
                [
                    int(row["timestamp"]),
                    bin_str,
                    count,
                    utils.INITIAL_STATE,
                    utils.COST_MATRIX,
                    utils.APPROACH,
                    utils.PLANT_NUM,
                    utils.PLANT_ROW,
                    str(datetime.fromtimestamp(row["timestamp"])) + " CEST",
                ]
            )

    new_df = pd.DataFrame(new_df, columns=utils.HUMIDITY_BIN)
    insert_with_string_io(new_df, "humidity_bins", conn)
    print("Done")
    return new_df


# Uploads into user_in_plant
def user_in_plant(conn):
    print("Uploading user_in_plant datas...")
    new_df = []
    new_df.append(
        [
            utils.SOURCE,
            utils.INITIAL_STATE,
            utils.COST_MATRIX,
            utils.APPROACH,
            utils.PLANT_NUM,
            utils.PLANT_ROW,
            utils.USER_ID,
            utils.WATERING_ADVICE,
        ]
    )
    new_df = pd.DataFrame(new_df, columns=utils.USER_IN_PLANT)
    insert_with_string_io(new_df, "user_in_plant", conn)
    print("Done")


# Uploads into interpolated_data
def load_interpolated_data(wc_path, conn):
    print(
        "Uploading interpolated data "
        + str(wc_path.split("/")[len(wc_path.split("/")) - 1])
        + " "
        + str(utils.INITIAL_STATE)
        + " - "
        + str(utils.COST_MATRIX)
        + " cost matrix - "
        + str(utils.APPROACH)
        + "..."
    )
    df = pd.read_csv(wc_path)
    outDF = pd.DataFrame(columns=["timestamp", "variable", "value"])

    melted = df.melt(
        col_level=0,
        id_vars=["timestamp"],
        value_vars=[x for x in df.columns if x != "timestamp"],
    )
    for v in melted["variable"].unique():
        f = melted[melted["variable"] == v]
        outDF = outDF.append(f, ignore_index=True)
    new_df = []
    for _, row in outDF.iterrows():
        sensor_id = row["variable"]
        z = int(sensor_id.split("_")[0].replace("z", ""))
        x = int(sensor_id.split("_")[2].replace("x", ""))
        new_df.append(
            [
                x,
                z,
                row["value"],
                utils.INITIAL_STATE,
                utils.COST_MATRIX,
                utils.APPROACH,
                utils.PLANT_NUM,
                utils.PLANT_ROW,
                int(row["timestamp"]),
                str(datetime.fromtimestamp(row["timestamp"])) + " CEST",
                0,
            ]
        )
    new_df = pd.DataFrame(new_df, columns=utils.INTERPOLATED)
    insert_with_string_io(new_df, "data_interpolated", conn)
    print("Done")


def insert_with_string_io(df: pd.DataFrame, schema, conn):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    with conn.cursor() as cursor:
        try:
            cursor.copy_expert(
                "COPY criteria.public."
                + schema
                + " FROM STDIN (FORMAT 'csv', HEADER false)",
                buffer,
            )
            conn.commit()
            print("... successfully pushed to db")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
