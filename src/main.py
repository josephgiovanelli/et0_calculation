import os
import json
import argparse

import pandas as pd

from utils.import_utils import readMeteoData
from et0.penman_monteith import computeHourlyET0
from et0.transmissivity import computeNormTransmissivity


def main(args):

    # Load properties
    with open(os.path.join(args.path, "properties.json")) as f:
        properties = json.loads(f.read())

    # Load weather data
    weatherData = readMeteoData(os.path.join(args.path, "meteo"))
    weatherData["time"] = pd.to_datetime(weatherData["timestamp"], unit="s")

    # Calculate ET0
    for weatherIndex in range(len(weatherData)):
        normTransmissivity = computeNormTransmissivity(
            weatherData,
            weatherIndex,
            properties["latitude"],
            properties["longitude"],
            properties["timezone"],
        )
        ET0 = computeHourlyET0(
            properties["altitude"],
            weatherData.loc[weatherIndex, "air_temperature"],
            weatherData.loc[weatherIndex, "solar_radiation"],
            weatherData.loc[weatherIndex, "air_humidity"],
            weatherData.loc[weatherIndex, "wind_speed"],
            normTransmissivity,
        )
        print(
            f"""timestamp: {weatherData.loc[weatherIndex, "timestamp"]}, ET0: {format(ET0, ".2f")}"""
        )


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
