from dataclasses import dataclass

from typing import List
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU

class Attr:
    def __init__(self, attr_name, attr_type) -> None:
        self.name = attr_name
        self.type = attr_type
   
@dataclass
class Table:
    domain: str
    tbl_id: str
    tbl_name: str
    t_attrs: List[Attr]
    s_attrs: List[Attr]
    num_columns: List[str]
    path: str=None
