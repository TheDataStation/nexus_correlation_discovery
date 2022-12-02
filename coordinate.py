import math
import re
import traceback
import pandas as pd
import censusgeocode as cg
from enum import IntEnum

class S_GRANU(IntEnum):
    BLOCK = 0
    BLOCK_GROUP = 1
    TRACT = 2
    COUNTY = 3

class Coordinate:
    """
    Contrary to the normal convention of "latitude, longitude", ordering in the coordinates property, GeoJSON and Well Known Text 
    order the coordinates as "longitude, latitude" (X coordinate, Y coordinate), as other GIS coordinate systems are encoded.
    """
    block = None
    block_group = None
    tract = None
    county = None

    def __init__(self, long, lat) -> None:
        self.long = long
        self.lat = lat
        self.resolve_resolution()
        self.full_resolution = [self.block, self.block_group, self.tract, self.county]

    def __hash__(self):
        return hash((self.long, self.lat))

    def __eq__(self, other):
        return math.isclose(self.long, other.long, rel_tol=1e-5) and math.isclose(self.lat, other.lat, rel_tol=1e-5)

    def resolve_resolution(self):
        location = cg.coordinates(x=self.long, y=self.lat)
        info = location['2020 Census Blocks'][0]
        self.block = info['BLOCK']
        self.block_group = info['BLKGRP']
        self.tract = info['TRACT']
        self.county = info['COUNTY']
    
    def transform(self, granu: S_GRANU):
        return self.full_resolution[granu:]


def parse_coordinate(str):
    if pd.isna(str):
        return None
    try:
        for match in re.findall(r'(?<=\().*?(?=\))', str):
            tokens = match.replace(',', ' ').split()
            if len(tokens) < 2:
                continue
            # wrong data, chicago's long, lat is around (-87 41)
            # pt[0]: longitude, pt[1] latitude
            pt = (float(tokens[0]), float(tokens[1]))
            if pt[0] > 0 and pt[1] < 0:
                return Coordinate(float(tokens[1]), float(tokens[0]))
            # return pt
            return Coordinate(float(tokens[0]), float(tokens[1]))
    except:
        print("string: ", str)
        # print("match: ", match)
        traceback.print_exc()
        return None
