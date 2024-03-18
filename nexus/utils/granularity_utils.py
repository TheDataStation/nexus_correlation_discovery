from typing import List

from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.time_point import TEMPORAL_GRANU


def get_inverted_index_names(temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU]):
    inverted_indices = []
    for temporal_granu in temporal_granu_l:
        inverted_indices.append(f"time_{temporal_granu.value}_inv")
    for spatial_granu in spatial_granu_l:
        inverted_indices.append(f"space_{spatial_granu.value}_inv")
    for temporal_granu in temporal_granu_l:
        for spatial_granu in spatial_granu_l:
            inverted_indices.append(f"time_{temporal_granu.value}_space_{spatial_granu.value}_inv")
    return inverted_indices