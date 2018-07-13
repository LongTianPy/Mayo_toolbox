#!/usr/bin/python
"""
"""

# IMPORT
import numpy as np
from scipy.stats import ttest_ind
import pandas as pd
import multiprocessing as mp

# VARIABLES
num_thread = mp.cpu_count()/2
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/"
cpg_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"
cpg_list_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_list.txt"

# FUNCTIONS
def perform_t_test(cpg_id):
    cpg_file = cpg_dir + cpg_id + ".txt"
    df = pd.read_table(cpg_file,sep=",",header=0)
    t, p = ttest_ind(df[df["TumorNormal"]=="Tumor"]["Value"],df[df["TumorNormal"]=="Normal"]["Value"],equal_var=False,nan_policy='omit')
    if p < 0.05:
        return cpg_id
    else:
        return 0



# MAIN
if __name__ == '__main__':
    pool = mp.Pool(num_thread)
    with open(cpg_list_file, "r") as f:
        lines = [i.strip().split("\t")[0] for i in f.readlines()]
    results = [pool.apply_async(perform_t_test,(cpg,)) for cpg in lines]
    get_result = [r.get() for r in results]
    with open(base_dir + "remaining_cpg.txt","w") as f:
        for i in get_result:
            if i != 0:
                f.write(i + "\n")