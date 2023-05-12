from dataclasses import dataclass

from typing import List


@dataclass
class Table:
    domain: str
    tbl_id: str
    tbl_name: str
    t_attrs: List[str]
    s_attrs: List[str]
    num_columns: List[str]
