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
    accumulator = 0
    df = pd.DataFrame()
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
        accumulator += ET0
        if pd.to_datetime(currentObs["timestamp"], unit="s").hour == 3:
            df = df.append(
                {
                    "timestamp": currentObs["timestamp"].astype(int),
                    "et0": accumulator,
                },
                ignore_index=True,
            )
            df.to_csv(
                os.path.join(args.path, "et0", "penman_monteith.csv"), index=False
            )
            accumulator = 0
        print(f"""timestamp: {currentObs["timestamp"]}, ET0: {format(ET0, ".2f")}""")


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
