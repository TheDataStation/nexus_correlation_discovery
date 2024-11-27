from nexus.data_ingestion.connection import ConnectionFactory
from nexus.data_search.data_polygamy import DataPolygamy
import nexus.utils.io_utils as io_utils
import numpy as np
from nexus.utils.data_model import (
    Table,
    Variable,
    AggFunc,
    SpatioTemporalKey,
    KeyType,
)
from nexus.data_ingestion.data_profiler import Profiler
from tqdm import tqdm
import time
import pandas as pd
import os
from nexus.utils import corr_utils
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict
import nexus.data_search.db_ops as db_ops
import math
from nexus.data_search.commons import FIND_JOIN_METHOD
from nexus.utils.coordinate import SPATIAL_GRANU
from nexus.utils.time_point import TEMPORAL_GRANU
from scipy.stats import pearsonr, spearmanr, kendalltau
from nexus.utils.profile_utils import is_num_column_valid
import pingouin as pg

# import warnings
# warnings.filterwarnings('error')

agg_col_profiles = None


@dataclass
class AggColumnProfile:
    missing_ratio: float
    zero_ratio: float
    missing_ratio_o: float
    zero_ratio_o: float
    cv: float

    def to_list(self):
        return [
            round(self.missing_ratio, 3),
            round(self.zero_ratio, 3),
            round(self.missing_ratio_o, 3),
            round(self.zero_ratio_o, 3),
            round(self.cv, 3),
        ]


class AggColumn:
    def __init__(
            self, domain, tbl_id, tbl_name, agg_name: str, agg_attr, col_data=None, description=None
    ) -> None:
        self.domain = domain
        self.tbl_id = tbl_id
        self.tbl_name = tbl_name
        self.agg_name = agg_name
        self.agg_attr = agg_attr
        if description:
            self.desc =description
        self.col_data = col_data

    def set_profile(self, tbl_profiles):
        # missing_ratio = col_data.isnull().sum() / len(col_data)
        # zero_ratio = (col_data == 0).sum() / len(col_data)
        # if the agg attr is using avg, calculate original missing and zero ratio
        missing_ratio_o, zero_ratio_o = 0, 0
        if self.agg_attr[0:3] == "avg":
            missing_ratio_o = tbl_profiles[self.agg_attr]["missing_ratio"]
            zero_ratio_o = tbl_profiles[self.agg_attr]["zero_ratio"]

        # cv = col_data.dropna().std() / col_data.dropna().mean()
        self.profile = AggColumnProfile(
            missing_ratio=0,
            zero_ratio=0,
            missing_ratio_o=missing_ratio_o,
            zero_ratio_o=zero_ratio_o,
            cv=0,
        )

    def get_stats(self, stat_name):
        return agg_col_profiles[self.agg_name][self.agg_attr[:-3]][stat_name]

    def get_id(self):
        return self.agg_name, self.agg_attr

    def to_list(self, metadata: Dict[str, str]=None):
        desc = ""
        if metadata:
            if self.agg_attr[:3] == 'avg':
                col_name = self.agg_attr[4:-3]
                if col_name in metadata:
                    desc = metadata[col_name] 
        return [
            self.domain,
            self.tbl_id,
            self.tbl_name,
            self.agg_name,
            self.agg_attr,
            desc,
        ] + self.profile.to_list()
        

class Correlation:
    def __init__(
            self,
            agg_col1: AggColumn,
            agg_col2: AggColumn,
            r_val: float,
            p_val: float,
            overlap: int,
            align_type,
    ):
        self.agg_col1 = agg_col1
        self.agg_col2 = agg_col2
        self.r_val = r_val
        self.r_val_impute_avg = 0
        self.r_val_impute_zero = 0
        self.p_val = p_val
        self.overlap = overlap
        self.align_type = align_type

    def set_impute_avg_r(self, res_sum):
        # print(res_sum)
        self.r_val_impute_avg = res_sum / (
            math.sqrt(
                self.agg_col1.get_stats("res_sum") * self.agg_col2.get_stats("res_sum")
            )
        )
        # print("rval", self.r_val_impute_avg)

    def set_impute_zero_r(self, n, inner_prod):
        n = self.agg_col1.get_stats("cnt") + self.agg_col2.get_stats("cnt") - n
        sum1, sum2 = self.agg_col1.get_stats("sum"), self.agg_col2.get_stats("sum")
        square_sum1, square_sum2 = self.agg_col1.get_stats(
            "sum_square"
        ), self.agg_col2.get_stats("sum_square")
        self.r_val_impute_zero = (n * inner_prod - sum1 * sum2) / (
                math.sqrt(n * square_sum1 - sum1 ** 2)
                * math.sqrt(n * square_sum2 - sum2 ** 2)
        )

    def to_list(self, metadata: Dict[str, str]):
        return (
                self.agg_col1.to_list(metadata)
                + self.agg_col2.to_list(metadata)
                + [
                    round(self.r_val, 3),
                    round(self.r_val_impute_avg, 3),
                    round(self.r_val_impute_zero, 3),
                    round(self.p_val, 3),
                    self.overlap,
                    self.align_type,
                ]
        )
    
    @staticmethod
    def from_csv(row):
        tbl_id1 = row['table_id1']
        tbl_name1 = row['table_name1']
        agg_tbl1 = row['agg_table1']
        agg_attr1 = row['agg_attr1']
        desc1 = row['description1']
        tbl_id2 = row['table_id2']
        tbl_name2 = row['table_name2']
        agg_tbl2 = row['agg_table2']
        agg_attr2 = row['agg_attr2']
        desc2 = row['description2']
        r_val = row['correlation coefficient']
        p_val = row['p value']
        overlap = row['number of samples']
        align_type = row['spatio-temporal key type']
        agg_col1 = AggColumn(domain='', tbl_id=tbl_id1, tbl_name=tbl_id1, agg_name=agg_tbl1, agg_attr=agg_attr1, description=desc1)
        agg_col2 = AggColumn(domain='', tbl_id=tbl_id2, tbl_name=tbl_id2, agg_name=agg_tbl2, agg_attr=agg_attr2, description=desc2)
        corr = Correlation(agg_col1=agg_col1, agg_col2=agg_col2, r_val=r_val, p_val=p_val, overlap=overlap, align_type=align_type)
        return corr
        


class CorrSearch:
    def __init__(
            self,
            conn_str: str,
            engine: str,
            data_sources: List[str],
            find_join_method,
            corr_method="MATRIX",
            impute_methods=[],
            explicit_outer_join=False,
            correct_method="FDR",
            q_val=None,
            joinable_lookup=None,
            mode=None,
            sketch_size=None,
    ) -> None:

        self.data_catalog = {}
        self.all_tbls = set()
        self.column_profiles = {}
        self.spatio_temporal_keys_by_type = None
        global agg_col_profiles
        agg_col_profiles = {}
        for data_source in data_sources:
            config = io_utils.load_config(data_source)
            attr_path = config["attr_path"]
            profile_path = config["profile_path"]
            agg_col_profile_path = config["col_stats_path"]
            self.data_catalog.update(io_utils.load_json(attr_path))
            self.all_tbls = self.all_tbls.union(set(self.data_catalog.keys()))
            self.column_profiles.update(io_utils.load_json(profile_path))
            agg_col_profiles.update(io_utils.load_json(agg_col_profile_path))
       
        self.db_engine = ConnectionFactory.create_connection(conn_str, engine)

        self.data = []
        self.count = 0
        self.visited_tbls = set()
        self.visited_keys = set()

        self.find_join_method = find_join_method
        # self.join_costs = join_costs

        self.join_all_cost = 0
        self.corr_method = corr_method
        self.r_methods = impute_methods
        self.outer_join = explicit_outer_join
        self.correct_method = correct_method
        self.q_val = q_val
        self.joinable_lookup = joinable_lookup
        self.mode = mode
        self.find_join_only = False
        if self.mode == 'sketch':
            self.sketch = True
            self.sketch_size = sketch_size
        else:
            self.sketch = False
            self.sketch_size = 0

        if self.mode == 'data_polygamy':
            self.dataPolygamy = DataPolygamy(conn_str, attr_path)

        self.joinable_pairs = []
        self.overhead = 0
        self.all_corrs = []
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correction": {"total": 0},
            "time_dump_csv": {"total": 0},
            "time_create_tmp_tables": {"total": 0},
            "corr_counts": {"before": 0, "after": 0},
            "corr_count": {"total": 0},
            "significant": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "not_significant": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "strategy": {"find_join": 0, "join_all": 0, "skip": 0, "sample_times": 0},
        }

    def set_find_join_only(self, find_join_only):
        self.find_join_only = find_join_only

    def set_join_cost(self, temporal_granu, spatial_granu, overlap_threshold):
        self.join_costs = Profiler.get_join_cost(self.db_engine, self.data_catalog, temporal_granu, spatial_granu, overlap_threshold)

    def dump_polygamy_rel_to_csv(self, data, dir_path, schema_id):
        df = pd.DataFrame(
            data,
            columns=[
                'align_attrs1',
                'agg_attr1',
                'align_attrs2',
                'agg_attr2',
                'score',
                'strength',
            ],
        )
        if not os.path.exists(dir_path):
            # create the directory if it does not exist
            os.makedirs(dir_path)

        df.to_csv("{}/corr_{}.csv".format(dir_path, schema_id))

    def dump_corrs_to_csv(self, data: List[Correlation], dir_path, schema_id):
        df = io_utils.load_corrs_to_df(data)

        if not os.path.exists(dir_path):
            # create the directory if it does not exist
            os.makedirs(dir_path)

        df.to_csv("{}/corr_{}.csv".format(dir_path, schema_id))

    def find_all_corr_for_all_tbls(
            self, granu_list, o_t, r_t, p_t, corr_type='pearson', fill_zero=False, dir_path=None, st_type=None,
            control_vars=[]
    ):
        t_granu, s_granu = granu_list[0], granu_list[1]
        # profiler = Profiler(self.data_source, [t_granu], [s_granu])
        if self.mode == 'data_polygamy':
            self.dataPolygamy.set_path(t_granu, s_granu)
            print(self.shuffle_num)
        self.spatio_temporal_keys_by_type = Profiler.load_all_spatio_temporal_keys(self.data_catalog, t_granu, s_granu,
                                                                                   type_aware=True)
        spatio_temporal_keys = Profiler.load_all_spatio_temporal_keys(self.data_catalog, t_granu, s_granu)
        sorted_st_schemas = []
        for tbl, spatio_temporal_key in spatio_temporal_keys:
            cnt = self.db_engine.get_row_cnt(tbl, spatio_temporal_key)
            sorted_st_schemas.append((tbl, spatio_temporal_key, cnt))
        sorted_st_schemas = sorted(sorted_st_schemas, key=lambda x: x[2], reverse=False)
        cur_idx = 0
        for tbl, spatio_temporal_key, cnt in tqdm(sorted_st_schemas):
            if st_type == 'time_space':
                if spatio_temporal_key.get_type() != KeyType.TIME and spatio_temporal_key.get_type() != KeyType.SPACE:
                    continue
            self.find_all_corr_for_a_spatio_temporal_key(tbl, spatio_temporal_key, o_t, r_t, p_t, fill_zero, corr_type=corr_type,
                                                         control_vars=control_vars)
            start = time.time()
            if dir_path:
                if self.mode == 'data_polygamy':
                    self.dump_polygamy_rel_to_csv(self.data, dir_path, spatio_temporal_key.get_agg_tbl_name(tbl))
                else:
                    self.dump_corrs_to_csv(self.data, dir_path, spatio_temporal_key.get_agg_tbl_name(tbl))
            # after a table is done, clear the data
            self.perf_profile["corr_count"]["total"] += len(self.data)
            self.all_corrs.extend(self.data)
            self.data.clear()
            time_used = time.time() - start
            self.perf_profile["time_dump_csv"]["total"] += time_used

    def find_all_corr_for_a_tbl(self, tbl_id: str,
                                temporal_granu: TEMPORAL_GRANU, spatial_granu: SPATIAL_GRANU,
                                overlap_t, corr_t, p_t, fill_zero,
                                corr_type='pearson', control_variables=[]):

        if not self.spatio_temporal_keys_by_type:
            self.spatio_temporal_keys_by_type = Profiler.load_all_spatio_temporal_keys(self.data_catalog,
                                                                                       temporal_granu,
                                                                                       spatial_granu, type_aware=True)

        table = Table.table_from_tbl_id(tbl_id, self.data_catalog)
        spatio_temporal_keys = table.get_spatio_temporal_keys([temporal_granu], [spatial_granu])

        for spatio_temporal_key in spatio_temporal_keys:
            self.find_all_corr_for_a_spatio_temporal_key(
                tbl_id, spatio_temporal_key, overlap_t, corr_t, p_t, fill_zero, corr_type, control_variables
            )

    def get_vars_for_tbl(self, tbl, suffix):
        tbl_agg_cols = self.data_catalog[tbl]["num_columns"]
        vars = []
        for agg_col in tbl_agg_cols:
            if len(agg_col) > 56:
                continue
            if is_num_column_valid(agg_col):
                vars.append(Variable(tbl, agg_col, AggFunc.AVG, "avg_{}".format(agg_col), suffix=suffix))
        if len(vars) == 0 or tbl == '85ca-t3if':
            vars.append(Variable(tbl, "*", AggFunc.COUNT, "count", suffix=suffix))
        return vars

    def join_two_tables_on_spatio_temporal_keys(self, tbl_id1: str, agg_name1: str, tbl_id2: str, agg_name2: str,
                                                overlap_threshold: int,
                                                use_outer_join: bool = False, use_sketch: bool = False, k: int = 0):
        variables1 = Table.table_from_tbl_id(tbl_id1, self.data_catalog).get_variables(suffix='t1')
        variables2 = Table.table_from_tbl_id(tbl_id2, self.data_catalog).get_variables(suffix='t2')
        names1 = [var.proj_name for var in variables1]
        names2 = [var.proj_name for var in variables2]

        if use_sketch:
            agg_name1 = f"{agg_name1}_sketch_{k}"
            agg_name2 = f"{agg_name2}_sketch_{k}"

        if use_outer_join:
            merged_outer, _ = self.db_engine.join_two_tables_on_spatio_temporal_keys(agg_name1,
                                                                                  variables1,
                                                                                  agg_name2,
                                                                                  variables2,
                                                                                  use_outer=True)
            merged = merged_outer.dropna(subset=["key1", "key2"])
            if merged is None or (len(merged) < overlap_threshold and not use_sketch):
                return None, None, None, None
            elif use_sketch and len(merged) < 3:
                return None, None, None, None
        else:
            merged, _ = self.db_engine.join_two_tables_on_spatio_temporal_keys(agg_name1,
                                                                            variables1,
                                                                            agg_name2,
                                                                            variables2,
                                                                            use_outer=False)
            if merged is None or (len(merged) < overlap_threshold and not use_sketch):
                return None, None
            elif use_sketch and len(merged) < 3:
                return None, None

        df1, df2 = merged[names1].astype(float).round(3), merged[names2].astype(float).round(3)
        df1, df2 = self.drop_constant_columns(df1), self.drop_constant_columns(df2)
        if use_outer_join:
            df1_outer, df2_outer = merged_outer[names1].astype(float).round(3), merged_outer[names2].astype(
                float).round(3)
            df1_outer, df2_outer = self.drop_constant_columns(df1_outer), self.drop_constant_columns(df2_outer)
            return df1, df2, df1_outer, df2_outer
        return df1, df2

    def determine_find_join_method(
            self, tbl, st_key: SpatioTemporalKey, threshold: int, v_cnt: int
    ):
        agg_tbl = st_key.get_agg_tbl_name(tbl)
        print(f"current table: {agg_tbl}")
        self.visited_tbls.add(tbl)
        self.visited_keys.add(agg_tbl)

        # estimated join_all cost
        join_cost = self.join_costs[st_key.get_agg_tbl_name(tbl)].cost
        print(f"estimated join cost is {join_cost}")

        joinable_spatio_temporal_keys = []
        aligned_join_keys = self.spatio_temporal_keys_by_type[st_key.get_type()]

        join_all_cost = 0
        for tbl2, st_key2 in aligned_join_keys:
            if tbl2 == tbl:
                continue

            agg_name2 = st_key2.get_agg_tbl_name(tbl2)

            if agg_name2 not in self.join_costs or agg_name2 in self.visited_keys:
                continue  # meaning it does not have enough keys
            cnt2 = self.join_costs[agg_name2].cnt
            join_all_cost += min(cnt2, v_cnt)
            joinable_spatio_temporal_keys.append((tbl2, st_key2))

        # estimate index_search cost
        row_to_read, max_joinable = self.db_engine.get_total_row_to_read_and_max_joinable_tables(
            tbl, st_key, threshold
        )

        if v_cnt >= 100000:
            coef = 4.2  # 7
        else:
            coef = 0.1  # 0.15
        index_search_overhead = coef * (v_cnt + row_to_read)
        self.index_search_over_head = row_to_read
        max_joinable = min(max_joinable, len(joinable_spatio_temporal_keys))

        find_join_cost = index_search_overhead + max_joinable * v_cnt
        print(
            f"row_to_read: {row_to_read}; join all cost: {join_all_cost}; find join cost: {find_join_cost}; max_joinable: {min(max_joinable, len(joinable_spatio_temporal_keys))}"
        )

        start = time.time()
        sample_ratio = 0.05
        sampled_rows = int(v_cnt * sample_ratio)

        if find_join_cost <= join_all_cost:
            return "FIND_JOIN", None
        elif index_search_overhead >= join_all_cost:
            return "JOIN_ALL", joinable_spatio_temporal_keys
        else:
            self.perf_profile["strategy"]["sample_times"] += 1
            candidates, total_elements_sampled = self.db_engine.estimate_joinable_candidates(
                tbl, st_key, threshold, sampled_rows
            )
            if total_elements_sampled != 0:
                scale_factor = row_to_read // total_elements_sampled
                print(
                    f"total_elements_sampled: {total_elements_sampled}, scale_factor: {scale_factor}"
                )
            else:
                scale_factor = 0

            joinable_estimate = 0
            avg_join_cost = 0
            for tbl, schema, overlap in candidates:
                cand = schema.get_agg_tbl_name(tbl)
                if cand not in self.join_costs:
                    continue
                if overlap * scale_factor >= threshold:
                    if cand not in self.visited_keys:
                        joinable_estimate += 1
                        avg_join_cost += min(v_cnt, self.join_costs[cand].cnt)
            if len(candidates) == 0 or joinable_estimate == 0:
                avg_join_cost = join_cost
            else:
                avg_join_cost = avg_join_cost / joinable_estimate
            print(
                f"index_search cost: {index_search_overhead + joinable_estimate * avg_join_cost}; joinable_estimate: {joinable_estimate}; join cost estimate: {avg_join_cost}"
            )
            print(f"step 5 takes {time.time() - start}")
            if (
                    index_search_overhead + joinable_estimate * avg_join_cost
                    <= join_all_cost
            ):
                return "FIND_JOIN", None
            else:
                return "JOIN_ALL", joinable_spatio_temporal_keys

    def find_joinable_lookup(self, tbl1, st_schema: SpatioTemporalKey, o_t):
        key = st_schema.get_agg_tbl_name(tbl1)
        if key not in self.joinable_lookup:
            return []
        candidates = self.joinable_lookup[key]
        candidates = [x[0] for x in candidates if x[1] >= o_t]
        res = []
        for cand in candidates:
            res.append((cand[:9], cand))
        return res

    def find_joinable_nexus(self, tbl_id1: str, st_key1: SpatioTemporalKey, overlap_threshold: int):
        v_cnt = self.join_costs[st_key1.get_agg_tbl_name(tbl_id1)].cnt
        if self.find_join_method == FIND_JOIN_METHOD.INDEX_SEARCH:
            aligned_keys, _ = self.db_engine.estimate_joinable_candidates(
                tbl_id1, st_key1, overlap_threshold
            )
        elif self.find_join_method == FIND_JOIN_METHOD.JOIN_ALL:
            aligned_keys = []
            # aligned_tbls = self.all_tbls
            aligned_join_keys = self.spatio_temporal_keys_by_type[st_key1.get_type()]
            for tbl_id2, st_key2 in aligned_join_keys:
                if tbl_id2 == tbl_id1:
                    continue
                agg_name2 = st_key2.get_agg_tbl_name(tbl_id2)
                if (
                        agg_name2 not in self.join_costs
                        or agg_name2 in self.visited_keys
                ):
                    continue  # meaning it does not have enough keys
                aligned_keys.append((tbl_id2, st_key2))

        elif self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
            s = time.time()
            method, all_joinable_spatio_temporal_keys = self.determine_find_join_method(
                tbl_id1, st_key1, overlap_threshold, v_cnt
            )
            self.overhead += time.time() - s
            print(f"choose {method}")
            if method == "FIND_JOIN":
                method = FIND_JOIN_METHOD.INDEX_SEARCH
                self.perf_profile["strategy"]["find_join"] += 1
                aligned_keys, _ = self.db_engine.estimate_joinable_candidates(
                    tbl_id1, st_key1, overlap_threshold
                )
            elif method == "JOIN_ALL":
                method = FIND_JOIN_METHOD.JOIN_ALL
                self.perf_profile["strategy"]["join_all"] += 1
                aligned_keys = all_joinable_spatio_temporal_keys
        res = []
        for info in aligned_keys:
            res.append((info[0], info[1].get_agg_tbl_name(info[0])))

        if self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
            return method, res
        return res

    def control_variables_for_correlations(self, control_vars: List[Variable], correlations: List[Correlation]):
        tbl_cols = defaultdict(list)
        agg_name_to_tbl_name = {}
        control_var_tbls = [var.tbl_id for var in control_vars]
        control_var_names = [var.var_name for var in control_vars]
        for correlation in correlations:
            if correlation.agg_col1.agg_attr not in control_var_names:
                tbl_cols[correlation.agg_col1.agg_name].append(
                    Variable(correlation.agg_col1.agg_name, correlation.agg_col1.agg_attr, var_name=correlation.agg_col1.agg_attr))
                agg_name_to_tbl_name[correlation.agg_col1.agg_name] = correlation.agg_col1.tbl_id
            
            if correlation.agg_col2.agg_attr not in control_var_names:
                tbl_cols[correlation.agg_col2.agg_name].append(
                    Variable(correlation.agg_col2.agg_name, correlation.agg_col2.agg_attr, var_name=correlation.agg_col2.agg_attr))
                agg_name_to_tbl_name[correlation.agg_col2.agg_name] = correlation.agg_col2.tbl_id
     
        tables = list(tbl_cols.keys())
        all_correlations = []
        for i in range(len(tables)):
            for j in range(i+1, len(tables)):
                if tables[i] in control_var_tbls or tables[j] in control_var_tbls:
                    continue
                agg_name1, agg_name2 = tables[i], tables[j]
                tbl_id1, tbl_id2 = agg_name_to_tbl_name[agg_name1], agg_name_to_tbl_name[agg_name2]
               
                cur_tbl_cols = defaultdict(list)
                for k, col_list in tbl_cols.items():
                    if k == agg_name1:
                        for x in col_list:
                            x.proj_name = f"{x.var_name}_t1"
                            cur_tbl_cols[k].append(x)
                    elif k == agg_name2:
                        for x in col_list:
                            x.proj_name = f"{x.var_name}_t2"
                            cur_tbl_cols[k].append(x)
                for var in control_vars:
                    cur_tbl_cols[var.tbl_id].append(Variable(var.tbl_id, var.attr_name, None, var.attr_name))
                names1 = [var.proj_name for var in cur_tbl_cols[agg_name1]]
                names2 = [var.proj_name for var in cur_tbl_cols[agg_name2]]
                df = self.db_engine.join_multi_agg_tbls(cur_tbl_cols)
                res = self.get_corrs_with_control_vars(
                    df,
                    tbl_id1, agg_name1, names1,
                    tbl_id2, agg_name2, names2,
                    control_var_names,
                    0, 0.05,
                    fill_zero=True, flag=None
                )
                # print(len(res))
                all_correlations.extend(res)
        return all_correlations
        
    
    def find_all_corr_for_a_spatio_temporal_key(
            self, tbl_id1: str, spatio_temporal_key: SpatioTemporalKey,
            overlap_threshold: int, corr_threshold: float, p_threshold: float,
            fill_zero: bool, corr_type='pearson',
            control_vars=[]
    ):
        self.join_all_cost = 0
        self.cur_join_time = 0
        flag = spatio_temporal_key.get_type().value
        # join method to be used
        method = self.find_join_method
        """
        Find aligned schemas whose overlap with the input st_schema is greater then o_t
        """
        start = time.time()
        self.visited_tbls.add(tbl_id1)
        agg_name1 = spatio_temporal_key.get_agg_tbl_name(tbl_id1)
        self.visited_keys.add(agg_name1)
        if agg_name1 not in self.join_costs:
            print("skip because this table does not have enough keys")
            self.perf_profile["strategy"]["skip"] += 1
            return

        if self.mode == 'data_polygamy':
            vars1 = self.dataPolygamy.get_vars(tbl_id1)
            feature_map = {}
            for var in vars1:
                pos, neg = self.dataPolygamy.load_features(agg_name1, var.var_name)
                if pos is not None and neg is not None:
                    feature_map[var.var_name] = (pos, neg)
                    if spatio_temporal_key.get_type() == KeyType.TIME_SPACE:
                        shuffle_num = self.st_shuffle_num
                    else:
                        shuffle_num = self.shuffle_num
                    for i in range(shuffle_num):
                        feature_map[f"{var.var_name}_{i}"] = self.dataPolygamy.load_features(agg_name1, var.var_name,
                                                                                             shuffle=i)

        if self.mode == 'sketch' or self.mode == 'data_polygamy':
            aligned_keys = self.find_joinable_lookup(tbl_id1, spatio_temporal_key, overlap_threshold)
        elif self.joinable_lookup and self.mode == 'lazo':
            aligned_keys = self.find_joinable_lookup(tbl_id1, spatio_temporal_key, overlap_threshold)
        elif self.joinable_lookup and self.mode == 'nexus':
            # exclude aligned keys that are not in lazo's result
            if self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
                method, aligned_keys = self.find_joinable_nexus(tbl_id1, spatio_temporal_key, overlap_threshold)
            aligned_schemas_lazo = self.find_joinable_lookup(tbl_id1, spatio_temporal_key, overlap_threshold)
            if method == FIND_JOIN_METHOD.INDEX_SEARCH:
                print("pruned")
                aligned_keys = [x for x in aligned_keys if x in aligned_schemas_lazo]
        else:
            if self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
                method, aligned_keys = self.find_joinable_nexus(tbl_id1, spatio_temporal_key, overlap_threshold)
            else:
                aligned_keys = self.find_joinable_nexus(tbl_id1, spatio_temporal_key, overlap_threshold)

        time_used = time.time() - start
        self.cur_find_join_time = time_used
        self.perf_profile["time_find_joins"]["total"] += time_used
        self.perf_profile["time_find_joins"][flag] += time_used

        """
        Begin to align and compute correlations
        """
        tbl_schema_corrs = []
        # print(method)
        if self.find_join_only and method == FIND_JOIN_METHOD.INDEX_SEARCH:
            print('Find_Join', len(aligned_keys))
            self.perf_profile["num_joins"]["total"] += len(aligned_keys)
            self.perf_profile["num_joins"][flag] += len(aligned_keys)
            return

        for tbl2, agg_name2 in aligned_keys:
            if tbl2 not in self.data_catalog:
                continue
            if self.find_join_only:
                start = time.time()
                overlap = db_ops.get_intersection(self.cur, agg_name1, agg_name2)
                time_used = time.time() - start
                if overlap >= overlap_threshold:
                    self.perf_profile["num_joins"]["total"] += 1
                    self.perf_profile["num_joins"][flag] += 1
                self.perf_profile["time_find_joins"]["total"] += time_used
                self.perf_profile["time_find_joins"][flag] += time_used
                continue

            if tbl2 == tbl_id1 or agg_name2 in self.visited_keys or agg_name2 not in agg_col_profiles:
                continue
            if self.mode == 'data_polygamy':
                vars2 = self.dataPolygamy.get_vars(tbl2)
                # agg_tbl2_df = db_ops.read_agg_tbl(self.cur, agg_name2, vars2)
                # funcs2 = self.dataPolygamy.get_functions(agg_tbl2_df, vars2)
                for var1 in vars1:
                    if var1.var_name not in feature_map:
                        continue
                    for var2 in vars2:
                        pos1, neg1 = feature_map[var1.var_name]
                        pos2, neg2 = self.dataPolygamy.load_features(agg_name2, var2.var_name)
                        # self.dataPolygamy.find_features(func2)
                        if pos2 is None or neg2 is None:
                            continue
                        score, strength = self.dataPolygamy.relationships(pos1, neg1, pos2, neg2)
                        if score != 0:
                            significant = True
                            if spatio_temporal_key.get_type() == KeyType.TIME_SPACE:
                                shuffle_num = self.st_shuffle_num
                            else:
                                shuffle_num = self.shuffle_num
                            for i in range(shuffle_num):
                                pos1, neg1 = feature_map[f"{var1.var_name}_{i}"]
                                pos2, neg2 = self.dataPolygamy.load_features(agg_name2, var2.var_name, shuffle=i)
                                score_shuffle, strength_shuffle = self.dataPolygamy.relationships(pos1, neg1, pos2,
                                                                                                  neg2)
                                if abs(score_shuffle) >= abs(score):
                                    self.perf_profile['not_significant']["total"] += 1
                                    self.perf_profile['not_significant'][flag] += 1
                                    if self.perf_profile['not_significant']["total"] % 1000 == 0:
                                        print("not significant", self.perf_profile['not_significant'])
                                    significant = False
                                    break
                            # print((agg_name1, var1+'_t1', agg_name2, var2 + '_t2', score, strength))
                            if significant:
                                self.perf_profile['significant']["total"] += 1
                                self.perf_profile['significant'][flag] += 1
                                if self.perf_profile['significant']["total"] % 1000 == 0:
                                    print("significant", self.perf_profile['significant'])
                                self.data.append((agg_name1, var1.var_name + '_t1', agg_name2, var2.var_name + '_t2',
                                                  score, strength))
                continue

            # Align two schemas
            start = time.time()
            df1_outer, df2_outer = None, None

            if not self.outer_join and len(control_vars) == 0:
                df1, df2 = self.join_two_tables_on_spatio_temporal_keys(
                    tbl_id1, agg_name1, tbl2, agg_name2, overlap_threshold, use_outer_join=False,
                    use_sketch=self.sketch, k=self.sketch_size
                )
                if df1 is None or df2 is None:
                    continue
            elif self.outer_join and len(control_vars) == 0:
                df1, df2, df1_outer, df2_outer = self.join_two_tables_on_spatio_temporal_keys(
                    tbl_id1, agg_name1, tbl2, agg_name2, overlap_threshold, use_outer_join=True,
                    use_sketch=self.sketch, k=self.sketch_size
                )
                if df1 is None or df2 is None:
                    continue
            elif len(control_vars) > 0:
                # need to join table1, table2 and the control variables together
                tbl_cols = defaultdict(list)
                tbl_cols[agg_name1] = self.get_vars_for_tbl(tbl_id1, suffix='t1')
                names1 = [var.proj_name[:63] for var in tbl_cols[agg_name1]]
                tbl_cols[agg_name2] = self.get_vars_for_tbl(tbl2, suffix='t2')
                names2 = [var.proj_name[:63] for var in tbl_cols[agg_name2]]
                for var in control_vars:
                    tbl_cols[var.tbl_id].append(Variable(var.tbl_id, var.attr_name, None, var.attr_name))
                control_var_names = [var.attr_name for var in control_vars]
                df = self.db_engine.join_multi_agg_tbls(tbl_cols)
                if len(df) < overlap_threshold:
                    continue
            time_used = time.time() - start
            self.cur_join_time += time_used
            self.perf_profile["time_join"]["total"] += time_used
            self.perf_profile["time_join"][flag] += time_used

            if self.joinable_lookup and self.mode == 'nexus':
                if (tbl2, agg_name2) not in aligned_schemas_lazo:
                    print("not in lazo")
                    continue

            if len(control_vars) == 0 and (df1 is None or df2 is None):
                continue

            if len(control_vars) > 0 and df is None:
                continue

            self.perf_profile["num_joins"]["total"] += 1
            self.perf_profile["num_joins"][flag] += 1

            # Calculate correlation
            start = time.time()
            res = []
            if self.corr_method == "MATRIX" and corr_type == 'pearson' and len(control_vars) == 0:
                res = self.get_corr_opt(
                    df1,
                    df2,
                    df1_outer,
                    df2_outer,
                    tbl_id1,
                    agg_name1,
                    tbl2,
                    agg_name2,
                    corr_threshold,
                    p_threshold,
                    fill_zero,
                    flag,
                )
            elif (self.corr_method == 'FOR_PAIR' or corr_type != 'pearson') and len(control_vars) == 0:
                res = self.get_corr_pairwise(
                    df1,
                    df2,
                    tbl_id1,
                    agg_name1,
                    tbl2,
                    agg_name2,
                    corr_threshold,
                    p_threshold,
                    corr_type,
                    fill_zero,
                    flag,
                )
            elif len(control_vars) > 0:
                res = self.get_corrs_with_control_vars(
                    df,
                    tbl_id1, agg_name1, names1,
                    tbl2, agg_name2, names2,
                    control_var_names,
                    corr_threshold, p_threshold,
                    fill_zero=fill_zero, flag=flag, corr_type=corr_type
                )
            if res is not None:
                tbl_schema_corrs.extend(res)
            time_used = time.time() - start
            self.perf_profile["time_correlation"]["total"] += time_used
            self.perf_profile["time_correlation"][flag] += time_used

        """
        Perform multiple-comparison correction
        """
        start = time.time()
        if self.correct_method == "FDR":
            tbl_schema_corrs = self.bh_correction(tbl_schema_corrs, corr_threshold)

        self.perf_profile["corr_counts"]["after"] += len(tbl_schema_corrs)
        self.perf_profile["time_correction"]["total"] += time.time() - start
        self.data.extend(tbl_schema_corrs)
        return method

    def bh_correction(self, corrs: List[Correlation], r_t):
        filtered_corrs = []
        # group correlations by their starting columns
        corr_groups = defaultdict(list)
        for corr in corrs:
            corr_groups[corr.agg_col1.get_id()].append(corr)

        for corr_group in corr_groups.values():
            # sort corr_group by p_value
            corr_group.sort(key=lambda a: a.p_val)
            n = len(corr_group)
            largest_i = -1
            for i, corr in enumerate(corr_group):
                bh_value = ((i + 1) / n) * self.q_val
                if corr.p_val < bh_value:
                    largest_i = i
            corrected_corr_group = []
            if largest_i >= 0:
                # print("largest i", largest_i)
                for corr in corr_group[0: largest_i + 1]:
                    if abs(corr.r_val) >= r_t:
                        corr.agg_col1.set_profile(
                            self.column_profiles[corr.agg_col1.tbl_id],
                        )
                        corr.agg_col2.set_profile(
                            self.column_profiles[corr.agg_col2.tbl_id],
                        )
                        corrected_corr_group.append(corr)
            filtered_corrs.extend(corrected_corr_group)
        return filtered_corrs

    def get_o_mean_mat(self, tbl, agg_name, df):
        stats = agg_col_profiles[agg_name]
        vec = []
        vec_dict = {}
        rows = len(df)
        names = df.columns
        for name in names:
            # print(name)
            _name = name[:-3]
            # remove invalid columns (columns that are all nulls or have only one non-null value)
            average = stats[_name]["avg"]
            res_sum = stats[_name]["res_sum"]
            if average is None or res_sum is None or res_sum == 0:
                df = df.drop(name, axis=1)
                continue
            vec.append(average)
            vec_dict[name] = average

        o_avg_mat = np.repeat([vec], rows, axis=0)
        return df, o_avg_mat, vec_dict

    def get_corr_opt(
            self,
            df1: pd.DataFrame,
            df2: pd.DataFrame,
            df1_outer: pd.DataFrame,
            df2_outer: pd.DataFrame,
            tbl1,
            agg_name1,
            tbl2,
            agg_name2,
            r_threshold,
            p_threshold,
            fill_zero,
            flag,
    ):
        global inner_prod_val
        res = []
        if fill_zero:
            df1, o_avg_mat1, avg_dict1 = self.get_o_mean_mat(tbl1, agg_name1, df1)
            df2, o_avg_mat2, avg_dict2 = self.get_o_mean_mat(tbl2, agg_name2, df2)
            names1, names2 = df1.columns, df2.columns
            if df1.shape[1] == 0 or df2.shape[1] == 0:
                # meaning there is no valid column in a table
                return None
            if self.outer_join:
                df1_outer, df2_outer = df1_outer[df1.columns], df2_outer[df2.columns]

            mat1, mat2 = df1.fillna(0).to_numpy(), df2.fillna(0).to_numpy()
            mat1_avg, mat2_avg = None, None
            if "impute_avg" in self.r_methods and not self.outer_join:
                mat1_avg, mat2_avg = (
                    df1.fillna(avg_dict1).to_numpy(),
                    df2.fillna(avg_dict2).to_numpy(),
                )
            mat_dict = corr_utils.mat_corr(
                mat1,
                mat2,
                mat1_avg,
                mat2_avg,
                o_avg_mat1,
                o_avg_mat2,
                names1,
                names2,
                False,
                self.r_methods,
                self.outer_join,
            )
            if self.outer_join:
                df1_outer, df2_outer = df1_outer[df1.columns], df2_outer[df2.columns]
                names1, names2 = df1_outer.columns, df2_outer.columns
                if "impute_avg" in self.r_methods:
                    mat_dict_outer = corr_utils.mat_corr(
                        df1_outer.fillna(df1_outer.mean()).to_numpy(),
                        df2_outer.fillna(df2_outer.mean()).to_numpy(),
                        mat1_avg,
                        mat2_avg,
                        o_avg_mat1,
                        o_avg_mat2,
                        names1,
                        names2,
                        False,
                        self.r_methods,
                        self.outer_join,
                    )
                    corr_impute_avg = mat_dict_outer["corrs"]
                if "impute_zero" in self.r_methods:
                    mat_dict_outer = corr_utils.mat_corr(
                        df1_outer.fillna(0).to_numpy(),
                        df2_outer.fillna(0).to_numpy(),
                        mat1_avg,
                        mat2_avg,
                        o_avg_mat1,
                        o_avg_mat2,
                        names1,
                        names2,
                        False,
                        self.r_methods,
                        self.outer_join,
                    )
                    corr_impute_zero = mat_dict_outer["corrs"]

            corr_mat = mat_dict["corrs"]
            pval_mat = mat_dict["p_vals"]
            if "impute_avg" in self.r_methods and not self.outer_join:
                res_sum_mat = mat_dict["res_sum"]
            if "impute_zero" in self.r_methods and not self.outer_join:
                inner_prod_mat = mat_dict["inner_product"]
        else:
            # use numpy mask array to ignore NaN values in the calculation
            df1_arr, df2_arr = df1.to_numpy(), df2.to_numpy()
            names1, names2 = df1.columns, df2.columns
            mat1 = np.ma.array(df1_arr, mask=np.isnan(df1_arr))
            mat2 = np.ma.array(df2_arr, mask=np.isnan(df2_arr))
            corr_mat, pval_mat = corr_utils.mat_corr(
                mat1, mat2, names1, names2, masked=True
            )
        # print(corr_mat)
        if self.correct_method == "FDR":
            # for fdr, we need all correlations regardless of
            # whether the corr coefficent exceeds the threhold or not.
            rows, cols = np.where(corr_mat >= -1)
        else:
            rows, cols = np.where(np.abs(corr_mat) >= r_threshold)
        index_pairs = [
            (corr_mat.index[row], corr_mat.columns[col]) for row, col in zip(rows, cols)
        ]
        for ix_pair in index_pairs:
            row, col = ix_pair[0], ix_pair[1]

            overlap = len(df1.index)  # number of samples that make up the correlation
            r_val = corr_mat.loc[row][col]
            p_val = pval_mat.loc[row][col]
            if "impute_avg" in self.r_methods and not self.outer_join:
                res_sum_val = res_sum_mat.loc[row][col]
            if "impute_zero" in self.r_methods and not self.outer_join:
                inner_prod_val = inner_prod_mat.loc[row][col]
            if abs(r_val) >= r_threshold:
                self.perf_profile["corr_counts"]["before"] += 1
            # if no correction is needed, prune based on the pval and rval
            if self.correct_method == "" or self.correct_method is None:
                if p_val > p_threshold or abs(r_val) < r_threshold:
                    continue
                    # for fdr correction, we need to include all correlations regardless of the p value
            agg_col1 = AggColumn(
                self.data_catalog[tbl1]["domain"], tbl1, self.data_catalog[tbl1]["name"], agg_name1, row, df1[row]
            )
            if self.correct_method is None or self.correct_method == "":
                agg_col1.set_profile(self.column_profiles[tbl1])
            agg_col2 = AggColumn(
                self.data_catalog[tbl2]["domain"], tbl2, self.data_catalog[tbl2]["name"], agg_name2, col, df2[col]
            )
            if self.correct_method is None or self.correct_method == "":
                agg_col2.set_profile(self.column_profiles[tbl2])
            new_corr = Correlation(agg_col1, agg_col2, r_val, p_val, overlap, flag)
            if "impute_avg" in self.r_methods and not self.outer_join:
                new_corr.set_impute_avg_r(res_sum_val)
            if "impute_zero" in self.r_methods and not self.outer_join:
                new_corr.set_impute_zero_r(mat1.shape[0], inner_prod_val)
            if "impute_avg" in self.r_methods and self.outer_join:
                new_corr.r_val_impute_avg = corr_impute_avg.loc[row][col]
            if "impute_zero" in self.r_methods and self.outer_join:
                new_corr.r_val_impute_zero = corr_impute_zero.loc[row][col]
            res.append(new_corr)
        return res

    def drop_constant_columns(self, df):
        # drop columns where all values are the same
        nunique = df.nunique()
        cols_to_drop = nunique[nunique == 1].index
        df = df.drop(cols_to_drop, axis=1)
        return df

    def get_corrs_with_control_vars(self, df, tbl1, agg_name1, var1_l, tbl2, agg_name2, var2_l, control_vars, r_t, p_t,
                                    fill_zero, flag, corr_type='pearson'):
        res = []
        if fill_zero:
            df = df.fillna(0)
        df = self.drop_constant_columns(df)
        for var1 in var1_l:
            if var1 not in df.columns:
                continue
            for var2 in var2_l:
                if var2 not in df.columns:
                    continue
                if var1[:-3] in control_vars or var2[:-3] in control_vars:
                    continue
                # import warnings
                # warnings.filterwarnings("error")
                # try:
                partial_corr = pg.partial_corr(data=df, x=var1, y=var2, covar=control_vars, method=corr_type).round(3)
                # except Warning as e:
                #     print(e)
                #     import traceback
                #     print(traceback.format_exc())
                #     print([var1, var2, *control_vars])
                #     print(df[[var1, var2, *control_vars]].columns)
                #     print(df[[var1, var2, *control_vars]].to_csv('debug.csv', index=False))
                #     break
                r_val, p_val = partial_corr['r'].iloc[0], partial_corr['p-val'].iloc[0]
                # TODO: it looks like -1 is also a invalid value in the partial correlation library, needs to verify this further
                if not r_val or np.isnan(r_val) or r_val == -1:
                    # print("continue 1")
                    # meaning undefined correlation coefficient such as constant array 
                    continue
                if self.correct_method == "" or self.correct_method is None:
                    if abs(r_val) < r_t:
                        continue
                if abs(r_val) >= r_t:
                    self.perf_profile["corr_counts"]["before"] += 1
                agg_col1 = AggColumn(
                    self.data_catalog[tbl1]["domain"], tbl1, self.data_catalog[tbl1]["name"], agg_name1, var1
                )
                if self.correct_method is None or self.correct_method == "":
                    agg_col1.set_profile(self.column_profiles[tbl1])
                agg_col2 = AggColumn(
                    self.data_catalog[tbl2]["domain"], tbl2, self.data_catalog[tbl2]["name"], agg_name2, var2
                )
                if self.correct_method is None or self.correct_method == "":
                    agg_col2.set_profile(self.column_profiles[tbl2])
                new_corr = Correlation(agg_col1, agg_col2, r_val, p_val, len(df), flag)
                res.append(new_corr)
        return res

    def get_corr_pairwise(
            self,
            df1: pd.DataFrame,
            df2: pd.DataFrame,
            tbl1,
            agg_name1,
            tbl2,
            agg_name2,
            r_t,
            p_t,
            corr_type,
            fill_zero,
            flag):
        res = []
        if fill_zero:
            df1, df2 = df1.fillna(0), df2.fillna(0)
        col_num1, col_num2 = len(df1.columns), len(df2.columns)
        mat1 = np.transpose(df1.to_numpy())
        mat2 = np.transpose(df2.to_numpy())
        for i in range(col_num1):
            col1 = mat1[i]
            if np.all(col1 == col1[0]):
                continue
            for j in range(col_num2):
                col2 = mat2[j]
                if np.all(col2 == col2[0]):
                    continue
                col_name1, col_name2 = df1.columns[i], df2.columns[j]
                r_val, p_val = None, None
                if corr_type == 'pearson':
                    r_val, p_val = pearsonr(col1, col2)
                elif corr_type == 'spearman':
                    r_val, p_val = spearmanr(col1, col2)
                elif corr_type == 'kendall':
                    r_val, p_val = kendalltau(col1, col2)
                if not r_val:
                    # meaning undefined correlation coefficient such as constant array 
                    continue
                if self.correct_method == "" or self.correct_method is None:
                    if abs(r_val) < r_t or p_val > p_t:
                        continue

                agg_col1 = AggColumn(
                    self.data_catalog[tbl1]["domain"], tbl1, self.data_catalog[tbl1]["name"], agg_name1, col_name1, col1
                )
                if self.correct_method is None or self.correct_method == "":
                    agg_col1.set_profile(self.column_profiles[tbl1])
                agg_col2 = AggColumn(
                    self.data_catalog[tbl2]["domain"], tbl2, self.data_catalog[tbl2]["name"], agg_name2, col_name2, col2
                )
                if self.correct_method is None or self.correct_method == "":
                    agg_col2.set_profile(self.column_profiles[tbl2])
                new_corr = Correlation(agg_col1, agg_col2, r_val, p_val, len(df1), flag)
                res.append(new_corr)
        return res


if __name__ == "__main__":
    granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]
    conn_str = "postgresql://yuegong@localhost/st_tables"
    data_source = "chicago_10k"
    config = io_utils.load_config(data_source)
    for granu_list in granu_lists:
        dir_path = "result/chicago_10k/day_block/"
        corr_search = CorrSearch(
            conn_str,
            data_source,
            FIND_JOIN_METHOD.INDEX_SEARCH,
            "AGG",
            "MATRIX",
            ["impute_avg", "impute_zero"],
            False,
            "FDR",
            0.05,
        )
        start = time.time()
        corr_search.find_all_corr_for_all_tbls(
            granu_list, o_t=10, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=dir_path
        )

        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        print(corr_search.perf_profile)
