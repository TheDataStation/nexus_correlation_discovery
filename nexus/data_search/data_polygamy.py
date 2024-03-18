from tqdm import tqdm
from nexus.data_search.search_db import DBSearch
import nexus.data_search.db_ops as db_ops
import numpy as np
from nexus.utils import io_utils
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.profile_utils import is_num_column_valid
import math
from nexus.data_ingestion.data_profiler import Profiler
from nexus.utils.time_point import TEMPORAL_GRANU
from nexus.utils.data_model import Variable, AggFunc, KeyType
import time
import collections

class DataPolygamy:
    def __init__(self, conn_str: str, attr_path: str):
        self.db_search = DBSearch(conn_str)
        self.cur = self.db_search.cur
        self.tbl_attrs = io_utils.load_json(attr_path)
    
    def set_path(self, t_granu, s_granu):
        self.path = f'evaluation/data_polygamy_indices/chicago_1m/{t_granu}_{s_granu}/'
        self.spatial_graph_path = f'evaluation/data_polygamy_indices/chicago_1m/spatial_graph_{t_granu}_{s_granu}.json'
        self.spatial_graph = io_utils.load_json(self.spatial_graph_path)
    
    def load_features(self, agg_tbl_name, var_name, shuffle=None):
        # if file does not exist, return None
        try:
            if shuffle is not None:
                features = io_utils.load_json(f'{self.path}/{agg_tbl_name}_{var_name}_shift_{shuffle}.json')
            else:
                features = io_utils.load_json(f'{self.path}/{agg_tbl_name}_{var_name}.json')
            # print(features['pos'], features['neg'])
            # print(f"{agg_tbl_name}_{var_name}_shift_{shuffle}")
            return set(features['pos']), set(features['neg'])
        except FileNotFoundError:
            # print("not found")
            return None, None
       
    def create_indices(self, data_source, t_granu, s_granu, dir_path, shuffle_num=2, st_shuffle_num=2):
        profiler = Profiler(data_source, [t_granu], [s_granu])
        st_schema_list = profiler.load_all_spatio_temporal_keys(profiler.data_catalog, t_granu, s_granu)
        threshold_map = {}
        # print(st_schema_list[0])
        for tbl, st_schema in tqdm(st_schema_list):
            agg_name = st_schema.get_agg_tbl_name(tbl)
            vars = self.get_vars(tbl)
            agg_tbl = db_ops.read_agg_tbl(self.cur, agg_name, vars)
            funcs = self.get_functions(agg_tbl, vars)
            first_func = funcs[0][1]
            keys = [x[0] for x in first_func]
            # print(keys[0:10])
            if st_schema.get_type() == KeyType.TIME_SPACE:
                keys = [tuple(x.split(',')) for x in keys]
                st_lookup_tbl, graph, t_index = self.create_st_graph(keys)
                org_bfs_order = self.st_bfs_order(keys, graph, t_index, st_lookup_tbl)
                print("keys", len(keys), "order", len(org_bfs_order))
                for j in range(st_shuffle_num):
                    # create a random shuffle of keys
                    np.random.shuffle(keys)
                    random_bfs_order = self.st_bfs_order(keys, graph, t_index, st_lookup_tbl)
                    for var_name, func in funcs:
                        theta1, theta2 = self.get_thresholds(func)
                        if theta1 is None or theta2 is None:
                            continue
                        # create original feature
                        pos, neg = self.find_features(func, theta1, theta2)
                        feature_map = {"pos": list(pos), "neg": list(neg)}
                        threshold_map[f"{agg_name}_{var_name}"] = (theta1, theta2, len(pos), len(neg))
                        io_utils.dump_json(f'{dir_path}/{agg_name}_{var_name}.json', feature_map)
                        # convert func to a map
                        func_map = {k: v for k, v in func}
                        random_func = [(','.join(org_bfs_order[i]), func_map[','.join(random_bfs_order[i])]) for i in range(len(org_bfs_order))]
                        pos, neg = self.find_features(random_func, theta1, theta2)
                        feature_map = {"pos": list(pos), "neg": list(neg)}
                        io_utils.dump_json(f'{dir_path}/{agg_name}_{var_name}_shift_{j}.json', feature_map)
            else:
                for var_name, func in funcs:
                    theta1, theta2 = self.get_thresholds(func)
                    if theta1 is None or theta2 is None:
                        continue
                    # create original feature
                    pos, neg = self.find_features(func, theta1, theta2)
                    feature_map = {"pos": list(pos), "neg": list(neg)}
                    threshold_map[f"{agg_name}_{var_name}"] = (theta1, theta2, len(pos), len(neg))
                    io_utils.dump_json(f'{dir_path}/{agg_name}_{var_name}.json', feature_map)
                    if st_schema.get_type() == KeyType.TIME:
                        func.sort(key=lambda x: x[0]) # sort by the temporal column
                        for i in range(shuffle_num):
                            offset = np.random.randint(1, len(func))
                            pos, neg = self.find_features(func, theta1, theta2, offset)
                            feature_map = {"pos": list(pos), "neg": list(neg)}
                            io_utils.dump_json(f'{dir_path}/{agg_name}_{var_name}_shift_{i}.json', feature_map)
                    elif st_schema.get_type() == KeyType.SPACE:
                        keys = [x[0] for x in func]
                        graph = self.get_sub_graph(set(keys))
                        org_bfs_order = self.spatial_bfs_order(keys, graph)
                        # convert func to a map
                        func_map = {k: v for k, v in func}
                        for j in range(shuffle_num):
                            # create a random shuffle of keys
                            np.random.shuffle(keys)
                            random_bfs_order = self.spatial_bfs_order(keys, graph)
                            random_func = [(org_bfs_order[i], func_map[random_bfs_order[i]]) for i in range(len(org_bfs_order))]
                            pos, neg = self.find_features(random_func, theta1, theta2)
                            feature_map = {"pos": list(pos), "neg": list(neg)}
                            io_utils.dump_json(f'{dir_path}/{agg_name}_{var_name}_shift_{j}.json', feature_map)

        io_utils.dump_json(f'{dir_path}/threshold_map.json', threshold_map)
    
    def create_st_graph(self, keys):
        spatial_keys = set([x[1] for x in keys])
        graph = self.get_sub_graph(spatial_keys)

        # get unique x[0] in x in increasing order
        unique_t = sorted(list(set([x[0] for x in keys])))
        t_index = {}
        for id, t in enumerate(unique_t):
            t_index[t] = id

        st_map = {}
        for key in keys:
            if key[0] not in st_map:
                st_map[key[0]] = set()
            st_map[key[0]].add(key[1])
        
        st_lookup_tbl = []
        for _, t in enumerate(unique_t):
            st_lookup_tbl.append((t, st_map[t]))
        
        return st_lookup_tbl, graph, t_index
    
    def st_bfs_order(self, keys, graph, t_index, st_lookup_tbl):
        visited = set()
        order = []
        for key in keys:
            # perform bfs
            if key in visited:
                continue
            queue = collections.deque([key])
            visited.add(key)
            while queue:
                cur = queue.popleft()
                order.append(cur)
                t_id = t_index[cur[0]]
                for s_key in st_lookup_tbl[t_id][1]:
                    if s_key in graph[cur[1]] and (cur[0], s_key) not in visited:
                        queue.append((cur[0], s_key))
                        visited.add((cur[0], s_key))
                if t_id > 0:
                    if cur[1] in st_lookup_tbl[t_id-1][1] and (st_lookup_tbl[t_id-1][0], cur[1]) not in visited:
                        queue.append((st_lookup_tbl[t_id-1][0], cur[1]))
                        visited.add((st_lookup_tbl[t_id-1][0], cur[1]))
                if t_id < len(st_lookup_tbl)-1:
                    if cur[1] in st_lookup_tbl[t_id+1][1] and (st_lookup_tbl[t_id+1][0], cur[1]) not in visited:
                        queue.append((st_lookup_tbl[t_id+1][0], cur[1]))
                        visited.add((st_lookup_tbl[t_id+1][0], cur[1]))
    
        return order

    def spatial_bfs_order(self, keys, graph):
        visited = set()
        order = []
        for key in keys:
            # perform bfs
            if key in visited:
                continue
            queue = collections.deque([key])
            visited.add(key)
            while queue:
                cur = queue.popleft()
                order.append(cur)
                for nei in graph[cur]:
                    if nei not in visited:
                        queue.append(nei)
                        visited.add(nei)
    
        return order

    def get_sub_graph(self, keys):
        sub_graph = {}
        for key in keys:
            sub_graph[key] = set()
            neighbors = self.spatial_graph[key]
            for nei in neighbors:
                if nei in keys:
                    sub_graph[key].add(nei)
        return sub_graph
    
    def relationships_between_tbls(self, tbl1, agg_name1, tbl2, agg_name2):
        vars1 = self.get_vars(tbl1)
        vars2 = self.get_vars(tbl2)
        agg_tbl1 = db_ops.read_agg_tbl(self.cur, agg_name1, vars1)
        agg_tbl2 = db_ops.read_agg_tbl(self.cur, agg_name2, vars2)
        funcs1 = self.get_functions(agg_tbl1, vars1)
        funcs2 = self.get_functions(agg_tbl2, vars2)
        for func1 in funcs1:
            for func2 in funcs2:
                score, strength = self.relationships_between_columns(func1[1], func2[1])
                print(agg_name1, func1[0], agg_name2, func2[0], score, strength)

    def get_vars(self, tbl):
        tbl1_agg_cols = self.tbl_attrs[tbl]["num_columns"]
        vars = []
        for agg_col in tbl1_agg_cols:
            if is_num_column_valid(agg_col):
                vars.append(Variable(tbl, agg_col, AggFunc.AVG, "avg_{}".format(agg_col)))
        if len(vars) == 0 or tbl == '85ca-t3if':
            vars.append(Variable(tbl, "*", AggFunc.COUNT, "count"))
        return vars

    def get_functions(self, agg_tbl1, vars):
        funcs = []
        for var in vars:
            new_df = agg_tbl1[['val', var.var_name]]
            func = [tuple(row) for row in new_df.itertuples(index=False)]
            funcs.append((var.var_name, func))
        return funcs

    def relationships(self, pos1, neg1, pos2, neg2):
        sigma1, sigma2 = pos1.union(neg1), pos2.union(neg2)
        sigma = (sigma1).intersection(sigma2)
        if len(sigma) == 0:
            return 0, 0
        p_num = len(pos1.intersection(pos2)) + len(neg1.intersection(neg2))
        n_num = len(pos1.intersection(neg2)) + len(neg1.intersection(pos2))
        score = (p_num - n_num)/len(sigma)
        tp = len(sigma1.intersection(sigma))
        fp = len(sigma1.difference(sigma))
        fn = len(sigma.difference(sigma1))
        # calculate precision and recall   
        precision = tp/(tp+fp)
        recall = tp/(tp+fn)
        strength = 2*precision*recall/(precision+recall)
        return score, strength
    
    def relationships_between_columns(self, func1, func2):
        pos1, neg1 = self.find_features(func1)
        pos2, neg2 = self.find_features(func2) 
        sigma1, sigma2 = pos1.union(neg1), pos2.union(neg2)
        sigma = (sigma1).intersection(sigma2)
        if len(sigma) == 0:
            return 0, 0
        p_num = len(pos1.intersection(pos2)) + len(neg1.intersection(neg2))
        n_num = len(pos1.intersection(neg2)) + len(neg1.intersection(pos2))
        score = (p_num - n_num)/len(sigma)
        tp = len(sigma1.intersection(sigma))
        fp = len(sigma1.difference(sigma))
        fn = len(sigma.difference(sigma1))
        # calculate precision and recall   
        precision = tp/(tp+fp)
        recall = tp/(tp+fn)
        if tp+fp == 0:
            print('precision invalid divide')
        if tp+fn == 0:
            print('recall invalid divide')
        if precision + recall == 0:
            print('precision and recall invalid divide')
        strength = 2*precision*recall/(precision+recall)
        return score, strength
    
    def find_features(self, func, theta1, theta2, offset=0):
        # theta1, theta2 = self.get_thresholds(func)
        if theta1 is None or theta2 is None:
            return None, None
        positives, negatives = [], []
        n = len(func)
        for i in range(n):
            v = func[i][1]
            k = func[(i-offset+n)%n][0]
            if math.isnan(v):
                continue
            if v > theta1:
                positives.append(k)
            elif v < theta2:
                negatives.append(k)
        return set(positives), set(negatives)

    def get_thresholds(self, func):
        values = [float(v) for _, v in func if v and not math.isnan(v)]
        if len(values) == 0:
            return None, None
        # calculate the standard deviation of the values
        std = np.std(values)
        if std == 0:
            return None, None
        # values = [v for k, v in func]
        theta1 = np.percentile(values, 75) 
        theta2 = np.percentile(values, 25) 
        return theta1, theta2

    def find_negative_features(self, func, theta):
        res = []
        for k, v in func:
            if v <= theta:
                res.append(k)
        return res

if __name__ == '__main__':
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    # conn_str = config["db_path"]
    conn_str = 'postgresql://yuegong@localhost/chicago_1m_new'
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    start = time.time()
    data_polygamy = DataPolygamy(conn_str, config['attr_path'])
    data_polygamy.set_path(t_granu, s_granu)
    data_polygamy.create_indices(data_source, t_granu, s_granu, f'evaluation/data_polygamy_indices/chicago_1m/{t_granu}_{s_granu}/', shuffle_num=4, st_shuffle_num=2)
    end = time.time()
    io_utils.dump_json(f'evaluation/runtime12_30/{data_source}/full_tables/polygamy_index_time_{t_granu}_{s_granu}.json', {'time': end - start})
    # data_polygamy.relationships_between_tbls("ijzp-q8t2", "ijzp-q8t2_date_3", "yqn4-3th2", "yqn4-3th2_violation_date_3")