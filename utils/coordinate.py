import math
import re
import traceback
import pandas as pd
import censusgeocode as cg
from enum import Enum
from shapely.geometry import Point
import geopandas as gpd
from typing import List
from config import SHAPE_PATH


class S_GRANU(Enum):
    BLOCK = 1
    BLG = 2
    TRACT = 3
    COUNTY = 4


class Coordinate:
    """
    Contrary to the normal convention of "latitude, longitude", ordering in the coordinates property, GeoJSON and Well Known Text
    order the coordinates as "longitude, latitude" (X coordinate, Y coordinate), as other GIS coordinate systems are encoded.
    """

    def __init__(self, point, block, block_group, tract, county):
        self.point = point
        self.block = block
        self.block_group = block_group
        self.tract = tract
        self.county = county
        self.full_resolution = [self.block, self.block_group, self.tract, self.county]

    def __hash__(self):
        return hash((self.long, self.lat))

    def __eq__(self, other):
        return math.isclose(self.long, other.long, rel_tol=1e-5) and math.isclose(
            self.lat, other.lat, rel_tol=1e-5
        )

    def resolve_resolution(self):
        location = cg.coordinates(x=self.long, y=self.lat)
        info = location["2020 Census Blocks"][0]
        self.block = info["BLOCK"]
        self.block_group = info["BLKGRP"]
        self.tract = info["TRACT"]
        self.county = info["COUNTY"]

    def transform(self, granu: S_GRANU):
        return list(reversed(self.full_resolution[granu - 1 :]))

    def to_str(self, repr: List[int]):
        return "".join([str(x) for x in repr])

    def to_int(self, repr: List[int]):
        return int("".join([str(x) for x in repr]))

    def transform_to_key(self, granu: S_GRANU):
        repr = self.full_resolution[granu - 1 :]
        return str(repr)


def transform(crd: Coordinate, granu: S_GRANU):
    return crd.full_resolution[granu - 1 :]


def parse_coordinate(str):
    if pd.isna(str):
        return None
    try:
        for match in re.findall(r"(?<=\().*?(?=\))", str):
            tokens = match.replace(",", " ").split()
            if len(tokens) < 2:
                continue
            # wrong data, chicago's long, lat is around (-87 41)
            # pt[0]: longitude, pt[1] latitude
            pt = (float(tokens[0]), float(tokens[1]))
            if pt[0] > 0 and pt[1] < 0:
                return Point(float(tokens[1]), float(tokens[0]))
            # return pt
            return Point(float(tokens[0]), float(tokens[1]))
    except:
        print("string: ", str)
        # print("match: ", match)
        traceback.print_exc()
        return None


def resolve_resolution_hierarchy(points, s_attr, shape_path: str):
    shapes = gpd.read_file(shape_path).to_crs(epsg=4326)
    df = gpd.sjoin(points, shapes, predicate="within")
    if len(df):
        df[s_attr] = df.apply(
            lambda row: Coordinate(
                row["geometry"],
                row["blockce10"],
                row["blockce10"][0],
                row["tractce10"],
                row["countyfp10"],
            ),
            axis=1,
        )
        return df
    else:
        return None


def resolve_spatial_hierarchy(points):
    """
    shape file can contain duplicate shapes, i.e.
    geometry number is different but all the other attributes are identical
    """
    shapes = gpd.read_file(SHAPE_PATH).to_crs(epsg=4326)
    df = gpd.sjoin(points, shapes, predicate="within")

    if len(df):
        df_resolved = df.apply(
            lambda row: Coordinate(
                row["geometry"],
                row["blockce10"],
                row["blockce10"][0],
                row["tractce10"],
                row["countyfp10"],
            ),
            axis=1,
        )
        return df_resolved[~df_resolved.index.duplicated(keep="first")].dropna()
    else:
        return None


def set_spatial_granu(crd: Coordinate, s_granu: S_GRANU):
    return crd.to_str(crd.transform(s_granu))


def pt_to_str(pt):
    pt = pt[1:-1]
    resolution = pt[::-1]
    return str(resolution)
