import os
import json
import argparse
import warnings

import pandas as pd

from utils import readMeteoData
from et0.penman_monteith import computeHourlyET0
from et0.transmissivity import computeNormTransmissivity

warnings.filterwarnings("ignore")


def main(args):

    # Load properties
    with open(os.path.join(args.path, "properties.json")) as f:
        properties = json.loads(f.read())

    # Load weather data
    weatherData = readMeteoData(os.path.join(args.path, "meteo"))
    weatherData["time"] = pd.to_datetime(weatherData["timestamp"], unit="s")

    # Calculate ET0
    et0_df = pd.DataFrame()
    for weatherIndex in range(len(weatherData)):
        currentObs = weatherData.loc[weatherIndex]
        normTransmissivity = computeNormTransmissivity(
            weatherData,
            weatherIndex,
            properties["latitude"],
            properties["longitude"],
            properties["timezone"],
        )
        ET0 = computeHourlyET0(
            properties["altitude"],
            currentObs["air_temperature"],
            currentObs["solar_radiation"],
            currentObs["air_humidity"],
            currentObs["wind_speed"],
            normTransmissivity,
        )
        et0_df = et0_df.append(
            {
                "timestamp": currentObs["timestamp"],
                "et0": ET0,
            },
            ignore_index=True,
        )
        print(f"""timestamp: {currentObs["timestamp"]}, ET0: {format(ET0, ".2f")}""")

    # Export ET0
    et0_df["group"] = et0_df.index / 24
    et0_df["group"] = et0_df["group"].astype(int)
    et0_df = et0_df.groupby("group").agg({"timestamp": "last", "et0": "sum"})
    et0_df["timestamp"] = et0_df["timestamp"].astype(int)
    et0_df.to_csv(os.path.join(args.path, "et0", "penman_monteith.csv"), index=False)


def parse_args():
    parser = argparse.ArgumentParser(description="et0 Calculation")

    parser.add_argument(
        "-p",
        "--path",
        type=str,
        required=False,
        default="data",
        help="path to data directory",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)
