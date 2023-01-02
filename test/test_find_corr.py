from st_api import API
import time

api = API()

start = time.time()
api.find_all_corr()

print("total time:", time.time() - start)