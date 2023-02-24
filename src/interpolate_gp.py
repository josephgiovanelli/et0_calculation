import os
import json
import argparse
import warnings

import pandas as pd
import numpy as np

from scipy.interpolate import interpn

warnings.filterwarnings("ignore")


def melt(df):
    melted_df = pd.DataFrame()
    for _, row in df.iterrows():
        for column in df.columns:
            if column != "timestamp":
                melted_df = melted_df.append(
                    {
                        "timestamp": row["timestamp"],
                        column.split("_")[0][0]: int(column.split("_")[0][1:]),
                        column.split("_")[1][0]: int(column.split("_")[1][1:]),
                        column.split("_")[2][0]: int(column.split("_")[2][1:]),
                        "value": row[column],
                    },
                    ignore_index=True,
                )
    return melted_df


def interpolate(group_key, group_df, support_df):
    interpolated_group_df = group_df.copy()
    interpolated_group_df = interpolated_group_df.sort_values(by=["x", "y", "z"])

    # Understand which interpolation to apply (1D, 2D, 3D)
    x = interpolated_group_df["x"].unique()
    y = interpolated_group_df["y"].unique()
    z = interpolated_group_df["z"].unique()

    # Instantiate and populate the data structure where all the known coordinates are stored
    points = []
    if len(x) > 1:
        points += [x]
    if len(y) > 1:
        points += [y]
    if len(z) > 1:
        points += [z]

    # Instantiate the data structure where all the values of the known points are stored
    if len(points) == 1:
        values = np.zeros(len(points[0]))
    if len(points) == 2:
        values = np.zeros((len(points[0]), len(points[1])))
    if len(points) == 3:
        values = np.zeros((len(points[0]), len(points[1]), len(points[2])))

    # Populate the data structure where all the values of the known points are stored
    for i in range(x.shape[-1]):
        for j in range(y.shape[-1]):
            for k in range(z.shape[-1]):
                value = interpolated_group_df[
                    (interpolated_group_df["x"] == x[i])
                    & (interpolated_group_df["y"] == y[j])
                    & (interpolated_group_df["z"] == z[k])
                ]["value"]
                if len(points) == 1:
                    # Even though we do not know which is the coordinate that has not one unique value,
                    # we can sum all of the coordinates because the index of the ones that has just one unique value would be 0
                    values[i + j + k] = value
                if len(points) == 2:
                    # We understand which is the pair of coordinates to consider, based on the one to not consider (lenght = 1)
                    if len(x) == 1:
                        values[j, k] = value
                    if len(y) == 1:
                        values[i, k] = value
                    if len(z) == 1:
                        values[i, j] = value
                if len(points) == 3:
                    values[i, j, k] = value

    # Calculate interpolated vales
    for i in range(
        int(support_df["x"]["min"]),
        int(support_df["x"]["max"] + support_df["x"]["step"]),
        int(support_df["x"]["step"]),
    ):
        for j in range(
            int(support_df["y"]["min"]),
            int(support_df["y"]["max"] + support_df["y"]["step"]),
            int(support_df["y"]["step"]),
        ):
            for k in range(
                int(support_df["z"]["min"]),
                int(support_df["z"]["max"] + support_df["z"]["step"]),
                int(support_df["z"]["step"]),
            ):
                if len(points) == 1:
                    point = np.array([i + j + k])
                if len(points) == 2:
                    if len(x) == 1:
                        point = np.array([j, k])
                    if len(y) == 1:
                        point = np.array([i, k])
                    if len(z) == 1:
                        point = np.array([i, j])
                if len(points) == 3:
                    point = np.array([i, j, k])
                value = interpn(points, values, point)
                if not (
                    (
                        (interpolated_group_df["x"] == i)
                        & (interpolated_group_df["y"] == j)
                        & (interpolated_group_df["z"] == k)
                    ).any()
                ):
                    interpolated_group_df = interpolated_group_df.append(
                        pd.DataFrame(
                            {
                                "timestamp": int(group_key),
                                "x": [int(i)],
                                "y": [int(j)],
                                "z": [int(k)],
                                "value": [float(value)],
                            }
                        ),
                        ignore_index=True,
                    )

    return interpolated_group_df


def expand_profile(interpolated_df, support_df):
    if support_df["x"]["extend"] != None:
        unique_zs = interpolated_df["z"].unique()
        unique_ys = interpolated_df["y"].unique()
        for z in unique_zs:
            for y in unique_ys:
                for x in range(
                    int(support_df["x"]["max"] + support_df["x"]["step"]),
                    int(support_df["x"]["extend"] + support_df["x"]["step"]),
                    support_df["x"]["step"],
                ):
                    record = interpolated_df[
                        (interpolated_df["z"] == z)
                        & (interpolated_df["y"] == y)
                        & (interpolated_df["x"] == support_df["x"]["max"])
                    ].copy()
                    record["x"] = x
                    interpolated_df = interpolated_df.append(record, ignore_index=True)

    if support_df["y"]["extend"] != None:
        unique_zs = interpolated_df["z"].unique()
        unique_xs = interpolated_df["x"].unique()
        for z in unique_zs:
            for x in unique_xs:
                for y in range(
                    int(support_df["y"]["max"] + support_df["y"]["step"]),
                    int((support_df["y"]["extend"] * -1) + support_df["y"]["step"]),
                    support_df["y"]["step"],
                ):
                    record = interpolated_df[
                        (interpolated_df["z"] == z)
                        & (interpolated_df["y"] == support_df["y"]["max"])
                        & (interpolated_df["x"] == x)
                    ].copy()
                    record["y"] = y
                    interpolated_df = interpolated_df.append(record, ignore_index=True)

    if support_df["z"]["extend"] != None:
        unique_ys = interpolated_df["y"].unique()
        unique_xs = interpolated_df["x"].unique()
        for y in unique_ys:
            for x in unique_xs:
                for z in range(
                    int(support_df["z"]["max"] + support_df["z"]["step"]),
                    int(support_df["z"]["extend"] + support_df["z"]["step"]),
                    support_df["z"]["step"],
                ):
                    record = interpolated_df[
                        (interpolated_df["z"] == support_df["z"]["max"])
                        & (interpolated_df["y"] == y)
                        & (interpolated_df["x"] == x)
                    ].copy()
                    record["z"] = z
                    interpolated_df = interpolated_df.append(record, ignore_index=True)
    return interpolated_df


def get_support_df(df):
    coordinates = [
        {
            column.split("_")[0][0]: int(column.split("_")[0][1:]),
            column.split("_")[1][0]: int(column.split("_")[1][1:]),
            column.split("_")[2][0]: int(column.split("_")[2][1:]),
        }
        for column in df.columns
        if column != "timestamp"
    ]

    support_df = {
        "x": {"max": float("-inf"), "min": float("inf"), "extend": 100, "step": 5},
        "y": {"max": float("-inf"), "min": float("inf"), "extend": None, "step": 5},
        "z": {"max": float("-inf"), "min": float("inf"), "extend": None, "step": 5},
    }

    for elem in coordinates:
        for key, value in elem.items():
            support_df[key]["max"] = max(value, support_df[key]["max"])
            support_df[key]["min"] = min(value, support_df[key]["min"])

    return support_df


def main(args):
    # Load data
    df = pd.read_csv(args.input_file)
    df = df.interpolate(method="linear", limit_direction="forward", axis=0)
    df = df.loc[:, pd.read_csv("data/raw_obs/water_potential.csv").columns]

    # Get a support_df
    support_df = get_support_df(df)

    # Melt the DataFrame
    melted_df = melt(df)

    # Interpolate the profile
    interpolated_df = pd.DataFrame()
    grouped_df = melted_df.groupby("timestamp")
    for group_key, group_df in grouped_df:
        interpolated_df = interpolated_df.append(
            interpolate(group_key, group_df, support_df), ignore_index=True
        )

    # Expand the profile
    expanded_df = expand_profile(interpolated_df, support_df)

    # Pivot the DataFrame
    pivoted_df = expanded_df.pivot(
        index="timestamp", columns=["z", "y", "x"], values="value"
    )
    pivoted_df.columns = [
        f"z{int(z)}_y{int(y)}_x{int(x)}" for z, y, x in pivoted_df.columns
    ]
    pivoted_df = pivoted_df.reset_index()
    pivoted_df["timestamp"] = pivoted_df["timestamp"].astype(int)

    # Load sim data and give the same column order
    pivoted_df = pivoted_df.loc[
        :,
        [
            x
            for x in list(pd.read_csv("data/sim/water_potential.csv").columns)
            if not (x.startswith("z5_") or x.startswith("z10_") or x.startswith("z15_"))
        ],
    ]

    # Export the DataFrame
    pivoted_df.to_csv(args.output_file, index=False)


def parse_args():
    parser = argparse.ArgumentParser(description="et0 Calculation")

    parser.add_argument(
        "-input-file",
        "--input-file",
        type=str,
        required=True,
        default="data",
        help="path to the input file",
    )

    parser.add_argument(
        "-output-file",
        "--output-file",
        type=str,
        required=True,
        default="data",
        help="path to the input file",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)
