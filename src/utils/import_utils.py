import pandas as pd
import os


def readMeteoData(meteoPath):
    humidityFile = "air_humidity.csv"
    humidity = pd.read_csv(os.path.join(meteoPath, humidityFile))

    radiationsFile = "solar_radiation.csv"
    radiations = pd.read_csv(os.path.join(meteoPath, radiationsFile))

    temperatureFile = "air_temperature.csv"
    temperature = pd.read_csv(os.path.join(meteoPath, temperatureFile))

    windFile = "wind_speed.csv"
    wind = pd.read_csv(os.path.join(meteoPath, windFile))

    meteoData = [humidity, radiations, temperature, wind]

    for i in range(len(meteoData) - 1):
        if meteoData[i].iloc[0]["timestamp"] != meteoData[i + 1].iloc[0]["timestamp"]:
            raise Exception("Meteo files have different time spans")

    for i in range(len(meteoData)):
        meteoData[i] = meteoData[i].set_index(["timestamp"])

    mergedDf = meteoData[0]
    for i in range(1, len(meteoData)):
        mergedDf = mergedDf.merge(
            meteoData[i], how="outer", left_index=True, right_index=True
        )

    return mergedDf.reset_index()


def readWaterData(waterPath, meteo_start, meteo_end):
    irrigationFile = "irrigation.csv"
    irrigation = pd.read_csv(os.path.join(waterPath, irrigationFile))

    precipitationsFile = "precipitation.csv"
    precipitations = pd.read_csv(os.path.join(waterPath, precipitationsFile))

    if irrigation.iloc[0]["timestamp"] != precipitations.iloc[0]["timestamp"]:
        raise Exception("Water files have different time spans")

    if irrigation.iloc[-1]["timestamp"] != precipitations.iloc[-1]["timestamp"]:
        raise Exception("Water files have different time spans")

    irrigation = irrigation.set_index(["timestamp"])
    precipitations = precipitations.set_index(["timestamp"])

    mergedDf = irrigation.merge(
        precipitations, how="outer", left_index=True, right_index=True
    )
    mergedDf = mergedDf.reset_index()

    if (
        meteo_start != mergedDf.iloc[0]["timestamp"]
        or meteo_end != mergedDf.iloc[-1]["timestamp"]
    ):
        raise Exception("Irrigation file has a different time span")

    return mergedDf
