from enum import Enum


class FIND_JOIN_METHOD(Enum):
    INDEX_SEARCH = "Index_Search"
    JOIN_ALL = "Join_All"
    COST_MODEL = "Cost_Model"
