#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import dask.dataframe as dd
from os import listdir
from os.path import isfile,join
import multiprocessing as mp
from scipy.stats import ttest_ind
import json

# VARIABLES
num_threads = mp.cpu_count()
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/"
cpg_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"

# FUNCTIONS
def run_ttest(file):
    df = pd.read_table(file,sep=",",header=0)
    t, p = ttest_ind(df[df["TumorNormal"] == "Tumor"]["Value"], df[df["TumorNormal"] == "Normal"]["Value"],
                     equal_var=False)
    return p


# MAIN
if __name__ == '__main__':
    files = [join(cpg_dir, f) for f in listdir(cpg_dir) if isfile(join(cpg_dir, f)) and f.endswith(".txt")]
    pool = {}
    for i in files:
        cpg_id = i.split("/")[-1][:-4]
        p = run_ttest(i)
        pool[cpg_id]=p
    js = json.dump(pool)
    f = open(base_dir + "pvalues.json","w")
    f.write(js)
    f.close()