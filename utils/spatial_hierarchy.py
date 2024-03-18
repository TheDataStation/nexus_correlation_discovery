from enum import Enum
from typing import Dict


class SPATIAL_GRANU(Enum):
    ALL = 0
    BLOCK = 1
    BLG = 2
    TRACT = 3
    COUNTY = 4
    STATE = 5
    ZIPCODE = 6


class SpatialHierarchy:
    def __init__(self, shape_file_path: str, granularity_map: Dict):
        self.shape_file_path = shape_file_path
        self.granularity_map = granularity_map

    @staticmethod
    def from_yaml(config):
        return SpatialHierarchy(config["shape_file_path"], config["granularity_map"])

    def to_yaml(self):
        return {
            "shape_file_path": self.shape_file_path,
            "granularity_map": {k.name: v for k, v in self.granularity_map.items()},
        }

