import io_utils
from config import META_PATH, DATA_PATH, INDEX_PATH
import pandas as pd
from coordinate import S_GRANU, pt_to_str
from time_point import T_GRANU, dt_to_str
from archived.search import search
import pandas as pd
from search_db import DBSearch


class API:
    def __init__(self, conn_str):
        self.meta_data = io_utils.load_json(META_PATH)
        self.load_meta_data()
        self.db_search = DBSearch(conn_str)

    def load_meta_data(self):
        self.tbl_lookup = {}
        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["domain"],
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            self.tbl_lookup[tbl_id] = [tbl_name, t_attrs, s_attrs]
        return self.tbl_lookup

    def get_tbl_name(self, tbl_id):
        return self.tbl_lookup[tbl_id][0]

    def show_all_tables(self):
        tbls = []

        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["domain"],
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            if t_attrs and s_attrs:
                for t_attr in t_attrs:
                    for s_attr in s_attrs:
                        tbl = [tbl_id, tbl_name, t_attr, s_attr]
                        tbls.append(tbl)
            elif t_attrs:
                for t_attr in t_attrs:
                    tbl = [tbl_id, tbl_name, t_attr, None]
                    tbls.append(tbl)
            else:
                for s_attr in s_attrs:
                    tbl = [tbl_id, tbl_name, None, s_attr]
                    tbls.append(tbl)
        df = pd.DataFrame(tbls, columns=["Table Id", "Table Name", "Time", "Location"])
        return df

    def search_tbl(
        self, tbl_id: str, t_attr: str, s_attr: str, t_granu: T_GRANU, s_granu: S_GRANU
    ):
        rows = []
        result = search(tbl_id, t_attr, s_attr, t_granu, s_granu)
        for tbl in result:
            tbl_id = tbl[0]
            rows.append([tbl_id, self.tbl_lookup[tbl_id][0]] + tbl[1:])

        if t_attr and s_attr:
            df = pd.DataFrame(
                rows, columns=["Table Id", "Table Name", "Time", "Location", "Overlap"]
            )
        elif t_attr:
            df = pd.DataFrame(
                rows, columns=["Table Id", "Table Name", "Time", "Overlap"]
            )
        else:
            df = pd.DataFrame(
                rows, columns=["Table Id", "Table Name", "Location", "Overlap"]
            )
        return df

    def preview_tbl(self, tbl_id: str):
        df = pd.read_csv(DATA_PATH + tbl_id + ".csv", nrows=50)
        return df

    def calculate_corr(self, tbl1, attrs1, tbl2, attrs2, granu_list):
        merged = self.db_search.aggregate_join_two_tables(
            tbl1, attrs1, tbl2, attrs2, granu_list
        )
        corr_matrix = merged.corr(method="pearson", numeric_only=True)
        return corr_matrix.iloc[1, 0]

    def find_all_corr(self):
        meta_data = io_utils.load_json(META_PATH)

        data = []
        for obj in meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["domain"],
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            if len(t_attrs) and len(s_attrs):
                for t_attr in t_attrs:
                    for s_attr in s_attrs:
                        aligned_tbls = search(
                            tbl_id, t_attr, s_attr, T_GRANU.MONTH, S_GRANU.TRACT
                        )
                        for tbl in aligned_tbls:
                            tbl_id2, t_attr2, s_attr2, overlap = (
                                tbl[0],
                                tbl[1],
                                tbl[2],
                                tbl[3],
                            )
                            if overlap >= 50:
                                print(tbl_id, t_attr, s_attr, tbl_id2, t_attr2, s_attr2)
                                corr = self.calculate_corr(
                                    tbl_id,
                                    t_attr,
                                    s_attr,
                                    tbl_id2,
                                    t_attr2,
                                    s_attr2,
                                    T_GRANU.MONTH,
                                    S_GRANU.TRACT,
                                )
                                if corr > 0.5:
                                    tbl_name, tbl_name2 = (
                                        self.tbl_lookup[tbl_id][0],
                                        self.tbl_lookup[tbl_id2][0],
                                    )
                                    print(
                                        tbl_id,
                                        tbl_name,
                                        t_attr,
                                        s_attr,
                                        tbl_id2,
                                        tbl_name2,
                                        t_attr2,
                                        s_attr2,
                                        corr,
                                    )
                                    data.append(
                                        [
                                            tbl_id,
                                            tbl_name,
                                            t_attr,
                                            s_attr,
                                            tbl_id2,
                                            tbl_name2,
                                            t_attr2,
                                            s_attr2,
                                            corr,
                                        ]
                                    )

        df = pd.DataFrame(
            data,
            columns=[
                "tbl_id1",
                "tbl_name1",
                "t_attr1",
                "s_attr1",
                "tbl_id2",
                "tbl_name2",
                "t_attr2",
                "s_attr2",
                "corr",
            ],
        )
        df.to_csv("corr.csv")
