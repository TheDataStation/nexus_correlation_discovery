import math
import re
import traceback
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from typing import List
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU, SpatialHierarchy


class Coordinate:
    """
    Contrary to the normal convention of "latitude, longitude", ordering in the coordinates property, GeoJSON and Well Known Text
    order the coordinates as "longitude, latitude" (X coordinate, Y coordinate), as other GIS coordinate systems are encoded.
    """

    def __init__(self, row, hierarchy=SpatialHierarchy):
        self.repr = {}
        for granularity, key in hierarchy.granularity_map.items():
            self.repr[granularity] = row[key]
       
    def __hash__(self):
        return hash((self.long, self.lat))

    def __eq__(self, other):
        return math.isclose(self.long, other.long, rel_tol=1e-5) and math.isclose(
            self.lat, other.lat, rel_tol=1e-5
        )

    def transform(self, granu: SPATIAL_GRANU):
        if granu == SPATIAL_GRANU.BLOCK:
            return [
                self.repr[SPATIAL_GRANU.STATE.name],
                self.repr[SPATIAL_GRANU.COUNTY.name],
                self.repr[SPATIAL_GRANU.TRACT.name],
                self.repr[SPATIAL_GRANU.BLOCK.name],
            ]
        elif granu == SPATIAL_GRANU.BLG:
            return [
                self.repr[SPATIAL_GRANU.STATE.name],
                self.repr[SPATIAL_GRANU.COUNTY.name],
                self.repr[SPATIAL_GRANU.TRACT.name],
                self.repr[SPATIAL_GRANU.BLG.name],
            ]
        elif granu == SPATIAL_GRANU.TRACT:
            return [
                self.repr[SPATIAL_GRANU.STATE.name],
                self.repr[SPATIAL_GRANU.COUNTY.name],
                self.repr[SPATIAL_GRANU.TRACT.name],
            ]
        elif granu == SPATIAL_GRANU.COUNTY:
            return [
                self.repr[SPATIAL_GRANU.STATE.name],
                self.repr[SPATIAL_GRANU.COUNTY.name],
            ]
        elif granu == SPATIAL_GRANU.STATE:
            return [self.repr[SPATIAL_GRANU.STATE.name]]
        elif granu == SPATIAL_GRANU.ZIPCODE:
            # print(list(self.repr.keys()))
            return [self.repr[SPATIAL_GRANU.ZIPCODE.name]]

    def to_str(self, repr: List[int]):
        return "-".join([str(x) for x in repr])

    def to_int(self, repr: List[int]):
        return int("".join([str(x) for x in repr]))

    def transform_to_key(self, granu: SPATIAL_GRANU):
        repr = self.full_resolution[granu - 1:]
        return str(repr)

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


def resolve_spatial_hierarchy(points, spatial_hierarchies: List[SpatialHierarchy], desired_s_granu: SPATIAL_GRANU):
    """
    shape file can contain duplicate shapes, i.e.
    geometry number is different but all the other attributes are identical
    """
    for spatial_hierarchy in spatial_hierarchies:
        if desired_s_granu.name not in spatial_hierarchy.granularity_map:
            continue
        shape_path = spatial_hierarchy.shape_file_path
        shapes = gpd.read_file(shape_path).to_crs(epsg=4326)
        df = gpd.sjoin(points, shapes, predicate="within")

        if len(df):
            df_resolved = df.apply(
                lambda row: Coordinate(row, spatial_hierarchy),
                axis=1,
            )

            return df_resolved[~df_resolved.index.duplicated(keep="first")].dropna()
        else:
            return None


def set_spatial_granu(crd: Coordinate, s_granu: SPATIAL_GRANU):
    res = crd.to_str(crd.transform(s_granu))
    # print(s_granu)
    # print(res)
    if res is pd.NA:
        print(crd.full_resolution)
    return res

