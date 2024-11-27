
from nexus.utils.time_point import TEMPORAL_GRANU
from nexus.utils.coordinate import SPATIAL_GRANU
from nexus.nexus_api import API
from nexus.utils.data_model import Variable
from sklearn import linear_model
import warnings
from nexus.corr_analysis.graph.graph_utils import filter_on_signals
from nexus.utils.io_utils import load_corrs_from_dir
from demo.cluster_utils import CorrCommunity
import pickle
import subprocess
import pandas as pd 

warnings. filterwarnings('ignore')
use_qgrid = False

def install_nexus():
    command = ["pip", "install", "-e", "."]
    try:
        subprocess.check_call(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Installation Nexus successful!")
    except subprocess.CalledProcessError as e:
        print("Installation Nexus failed:", e)

def find_all_correlations(t_granu, s_granu):
    # corr_path = "demo/chicago_month_tract/"
    # load correlations: corrs is a list of correlations; corr_map is map from correlated variables to their correlation coefficients
    # corrs, corr_map = load_corrs_from_dir(corr_path) 
    corrs = pd.read_csv('demo/chicago_month_census_tract.csv')
    corrs.rename(columns={'tbl_id1': 'table_id1', 'tbl_id2': 'table_id2', 
                          'tbl_name1': 'table_name1', 'tbl_name2': 'table_name2'}, inplace=True)
    return corrs

def get_correlation_communities(corrs, signal_thresholds):
    corr_community = CorrCommunity(corrs, 'chicago')
    corr_community.get_correlation_communities_chicago(signal_thresholds)
    return corr_community