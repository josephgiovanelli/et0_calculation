# transmissivity.py

import math
import datetime
import pandas as pd

SOLAR_CONSTANT = 1367  # [W/m²]
TRANSMISSIVITY_THRESHOLD = 300  # [W/m²]
MAXIMUM_TRANSMISSIVITY = 0.75


def degreeToRadians(degree):
    return degree * (math.pi / 180.0)


def dateToDOY(year, month, day):
    myDate = datetime.date(year, month, day)
    januaryFirst = datetime.date(year, 1, 1)
    return (myDate - januaryFirst).days + 1


# return clear sky solar radiation
def clearSkyRad(myDate, finalHourUTC, latDegrees, lonDegrees, timezone):
    latRad = degreeToRadians(latDegrees)
    longitudeCorrection = (timezone * 15.0 - lonDegrees) / 15.0

    solarTime = finalHourUTC - 0.5 + timezone
    if solarTime < 0:
        solarTime += 24
        myDate = myDate - 1
    if solarTime > 24:
        solarTime -= 24
        myDate = myDate - 1

    doy = dateToDOY(myDate.year, myDate.month, myDate.day)
    timeAdjustment = degreeToRadians(279.575 + 0.986 * doy)

    timeEq = (
        -104.7 * math.sin(timeAdjustment)
        + 596.2 * math.sin(2.0 * timeAdjustment)
        + 4.3 * math.sin(3.0 * timeAdjustment)
        - 12.7 * math.sin(4 * timeAdjustment)
        - 429.3 * math.cos(timeAdjustment)
        - 2.0 * math.cos(2.0 * timeAdjustment)
        + 19.3 * math.cos(3.0 * timeAdjustment)
    ) / 3600.0

    solarNoon = 12.0 + longitudeCorrection - timeEq

    solarDeclination = 0.4102 * math.sin(2.0 * math.pi / 365.0 * (doy - 80.0))

    solarAngle = math.asin(
        math.sin(latRad) * math.sin(solarDeclination)
        + math.cos(latRad)
        * math.cos(solarDeclination)
        * math.cos(math.pi / 12 * (solarTime - solarNoon))
    )
    clearSkyRadiation = (
        max(0.0, SOLAR_CONSTANT * math.sin(solarAngle)) * MAXIMUM_TRANSMISSIVITY
    )
    return clearSkyRadiation


def computeNormTransmissivity(obsData, obsIndex, latitude, longitude, timezone):
    currentIndex = obsIndex
    potentialRad = 0
    observedRad = 0
    nrHoursAhead = 0

    while (
        (potentialRad < TRANSMISSIVITY_THRESHOLD)
        and (nrHoursAhead < 12)
        and (currentIndex >= 0)
    ):
        currentData = obsData.iloc[currentIndex]
        currentDateTime = pd.to_datetime(currentData["timestamp"], unit="s")
        date = datetime.date(
            currentDateTime.year, currentDateTime.month, currentDateTime.day
        )
        hour = currentDateTime.hour
        potentialRad += clearSkyRad(date, hour, latitude, longitude, timezone)
        observedRad += currentData["solar_radiation"]
        if potentialRad < TRANSMISSIVITY_THRESHOLD:
            nrHoursAhead += 1
            currentIndex -= 1

    for i in range(nrHoursAhead):
        currentIndex = obsIndex + i
        if currentIndex < len(obsData):
            currentData = obsData.iloc[currentIndex]
            currentDateTime = pd.to_datetime(currentData["timestamp"], unit="s")
            date = datetime.date(
                currentDateTime.year, currentDateTime.month, currentDateTime.day
            )
            hour = currentDateTime.hour
            potentialRad += clearSkyRad(date, hour, latitude, longitude, timezone)
            observedRad += currentData["solar_radiation"]

    if potentialRad == 0:
        return 1.0
    else:
        return min(1.0, observedRad / potentialRad)
