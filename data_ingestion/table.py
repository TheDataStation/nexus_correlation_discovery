from dataclasses import dataclass

from typing import List

@dataclass
class Attr:
    name: str
    granu: str

@dataclass
class Table:
    domain: str
    tbl_id: str
    tbl_name: str
    t_attrs: List[Attr]
    s_attrs: List[Attr]
    num_columns: List[str]
    link: str
