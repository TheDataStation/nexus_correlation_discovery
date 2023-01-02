import io_utils
from config import META_PATH, DATA_PATH, INDEX_PATH
import pandas as pd
from coordinate import S_GRANU, pt_to_str
from mydatetime import T_GRANU, dt_to_str
from search import search
import pandas as pd
import os

class API:
    def __init__(self):
        self.meta_data = io_utils.load_json(META_PATH)
        self.load_meta_data()

    def load_meta_data(self):
        self.tbl_lookup = {}
        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = obj['domain'], obj['tbl_id'], obj['tbl_name'], obj['t_attrs'], obj['s_attrs']
            self.tbl_lookup[tbl_id] = [tbl_name, t_attrs, s_attrs]
        return self.tbl_lookup

    def get_tbl_name(self, tbl_id):
        return self.tbl_lookup[tbl_id][0]

    def show_all_tables(self):
        tbls = []

        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = obj['domain'], obj['tbl_id'], obj['tbl_name'], obj['t_attrs'], obj['s_attrs']
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
        df = pd.DataFrame(tbls, columns=['Table Id', 'Table Name', 'Time', 'Location'])
        return df
    
    def search_tbl(self, tbl_id: str, t_attr: str, s_attr: str, t_granu: T_GRANU, s_granu: S_GRANU):
        rows = []
        result = search(tbl_id, t_attr, s_attr, t_granu, s_granu)
        for tbl in result:
            tbl_id = tbl[0]
            rows.append([tbl_id, self.tbl_lookup[tbl_id][0]] + tbl[1:])
        
        if t_attr and s_attr:
            df = pd.DataFrame(rows, columns=['Table Id', 'Table Name', 'Time', 'Location', 'Overlap'])
        elif t_attr:
            df = pd.DataFrame(rows, columns=['Table Id', 'Table Name', 'Time', 'Overlap'])
        else:
            df = pd.DataFrame(rows, columns=['Table Id', 'Table Name', 'Location', 'Overlap'])
        return df

    def get_overlap_between_two_tables(self, tbl1: str, t_attr1: str, s_attr1: str, tbl2: str, t_attr2: str, s_attr2: str, t_granu: T_GRANU, s_granu: S_GRANU):
        if t_granu and s_granu:
            resolution = str([t_granu, s_granu])
        elif t_granu:
            resolution = str(int(t_granu))
        else:
            resolution = str(int(s_granu))

        print(resolution)
        tbl1_idx_path = self.get_idx_path(tbl1, t_attr1, s_attr1)
        tbl1_idx = io_utils.load_json(tbl1_idx_path)[resolution]
        tbl2_idx_path = self.get_idx_path(tbl2, t_attr2, s_attr2)
        tbl2_idx = io_utils.load_json(tbl2_idx_path)[resolution]
        overlap_units = tbl1_idx.keys() & tbl2_idx.keys()
        
        rows = []
        for unit in overlap_units:
            if t_granu and s_granu:
                rows.append(list(unit))
            elif t_granu:
                rows.append(dt_to_str(unit))
            else:
                rows.append(pt_to_str(unit))
        if t_granu and s_granu:
            df =  pd.DataFrame(rows, columns=['Time', 'Location'])
            df['Time'] = df['Time'].apply(dt_to_str)
            df['Location'] = df['Location'].apply(pt_to_str)
            return df
        elif t_granu:
            df =  pd.DataFrame(rows, columns=['Time'])
            return df
        else:
            df =  pd.DataFrame(rows, columns=['Location'])
            return df

    def preview_tbl(self, tbl_id: str):
        df = pd.read_csv(DATA_PATH + tbl_id + '.csv', nrows=50)
        return df
    
    def get_idx_path(self, tbl_id, t_attr, s_attr):
        if t_attr and s_attr:
            primary_path = os.path.join(INDEX_PATH, "index_{} {} {}.json".format(tbl_id, t_attr, s_attr)) 
        elif t_attr:
            primary_path = os.path.join(INDEX_PATH, "index_{} {}.json".format(tbl_id, t_attr)) 
        else:
            primary_path = os.path.join(INDEX_PATH, "index_{} {}.json".format(tbl_id, s_attr)) 

        return primary_path

    def full_join(self, tbl1: str, t_attr1: str, s_attr1: str, tbl2: str, t_attr2: str, s_attr2: str, t_granu: T_GRANU, s_granu: S_GRANU):
        if t_granu and s_granu:
            resolution = (t_granu, s_granu)
        elif t_granu:
            resolution = t_granu
        else:
            resolution = s_granu

        tbl1_idx_path = self.get_idx_path(tbl1, t_attr1, s_attr1)
        tbl1_idx = io_utils.load_pickle(tbl1_idx_path)[resolution]
        tbl2_idx_path = self.get_idx_path(tbl2, t_attr2, s_attr2)
        tbl2_idx = io_utils.load_pickle(tbl2_idx_path)[resolution]
        overlap_units = tbl1_idx.keys() & tbl2_idx.keys()
        
        df1 = pd.read_csv(DATA_PATH + tbl1 + '.csv')
        df2 = pd.read_csv(DATA_PATH + tbl2 + '.csv')

        merged_list = []
        for unit in overlap_units:
            tbl1_df = self.load_df(df1, tbl1_idx[unit], unit, t_granu, s_granu)
            tbl2_df = self.load_df(df2, tbl2_idx[unit], unit, t_granu, s_granu)
            merged = tbl1_df.merge(tbl2_df, how='cross')
            merged_list.append(merged.reset_index(drop=True))

        return pd.concat(merged_list)

    def load_df(self, df, row_indices, unit, t_granu, s_granu):
        tbl_df =  df.take(row_indices)
        if t_granu and s_granu:
            tbl_df['__DATE__'] = dt_to_str(unit[0])
            tbl_df['__LOCATION__'] = pt_to_str(unit[1])
        elif t_granu:
            tbl_df['__DATE__'] = dt_to_str(unit[0])
        else:
            tbl_df['__LOCATION__'] = pt_to_str(unit[1])
        return tbl_df

    def agg_join(self, tbl1: str, t_attr1: str, s_attr1: str, tbl2: str, t_attr2: str, s_attr2: str, t_granu: T_GRANU, s_granu: S_GRANU):
        if t_granu and s_granu:
            resolution = str([t_granu, s_granu])
        elif t_granu:
            resolution = str(int(t_granu))
        else:
            resolution = str(int(s_granu))

        tbl1_idx_path = self.get_idx_path(tbl1, t_attr1, s_attr1)
        tbl1_idx = io_utils.load_json(tbl1_idx_path)[resolution]
        tbl2_idx_path = self.get_idx_path(tbl2, t_attr2, s_attr2)
        tbl2_idx = io_utils.load_json(tbl2_idx_path)[resolution]
       
        overlap_units = tbl1_idx.keys() & tbl2_idx.keys()
        rows = []
        for unit in overlap_units:
            if t_granu and s_granu:
                row = [unit, len(tbl1_idx[unit]), len(tbl2_idx[unit])]
            else:
                row = [unit, len(tbl1_idx[unit]), len(tbl2_idx[unit])]
            rows.append(row)
        if t_granu and s_granu:
            df =  pd.DataFrame(rows, columns=['Time and Location', 'Count tbl1', 'Count tbl2'])
            # df['Time'] = df['Time'].apply(dt_to_str)
            # df['Location'] = df['Location'].apply(pt_to_str)
            return df
        elif t_granu:
            df = pd.DataFrame(rows, columns=['Time', 'Count tbl1', 'Count tbl2'])
            # df['Time'] = df['Time'].apply(dt_to_str)
            return df
        else:
            df = pd.DataFrame(rows, columns=['Location', 'Count tbl1', 'Count tbl2'])
            # df['Location'] = df['Location'].apply(pt_to_str)
            return df
    
    def calculate_corr(self, tbl1, t_attr1, s_attr1, tbl2, t_attr2, s_attr2, t_granu, s_granu):
        merged = self.agg_join(tbl1, t_attr1, s_attr1, tbl2, t_attr2, s_attr2, t_granu, s_granu)
        corr_matrix = merged.corr(method ='pearson', numeric_only=True)
        return corr_matrix.iloc[1, 0]
    
    def find_all_corr(self):
        meta_data = io_utils.load_json(META_PATH)
       
        data = []
        for obj in meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = obj['domain'], obj['tbl_id'], obj['tbl_name'], obj['t_attrs'], obj['s_attrs']
            if len(t_attrs) and len(s_attrs):
                for t_attr in t_attrs:
                    for s_attr in s_attrs:
                        aligned_tbls = search(tbl_id, t_attr, s_attr, T_GRANU.MONTH, S_GRANU.TRACT)
                        for tbl in aligned_tbls:
                            tbl_id2, t_attr2, s_attr2, overlap = tbl[0], tbl[1], tbl[2], tbl[3]
                            if overlap >= 50:
                                print(tbl_id, t_attr, s_attr, tbl_id2, t_attr2, s_attr2)
                                corr = self.calculate_corr(tbl_id, t_attr, s_attr, tbl_id2, t_attr2, s_attr2, T_GRANU.MONTH, S_GRANU.TRACT)
                                if corr > 0.5:
                                    tbl_name, tbl_name2 = self.tbl_lookup[tbl_id][0], self.tbl_lookup[tbl_id2][0]
                                    print(tbl_id, tbl_name, t_attr, s_attr, tbl_id2, tbl_name2, t_attr2, s_attr2, corr)
                                    data.append([tbl_id, tbl_name, t_attr, s_attr, tbl_id2, tbl_name2, t_attr2, s_attr2, corr])
                                   
        df = pd.DataFrame(data, columns = ["tbl_id1", "tbl_name1", "t_attr1", "s_attr1", "tbl_id2", "tbl_name2", "t_attr2", "s_attr2", "corr"])
        df.to_csv('corr.csv')




             

