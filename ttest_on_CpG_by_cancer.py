#!/usr/bin/python
"""
"""

# IMPORT
import numpy as np
from scipy.stats import ttest_ind
import pandas as pd
import multiprocessing as mp
import os

# VARIABLES
num_thread = mp.cpu_count()/2
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/"
cpg_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"
cpg_list_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_list.txt"
samples_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/samples.txt"
acronym_file="/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Acronyms.txt"
status_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Tumor_Normal.txt"
pvalue_table_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/pvalue_tables/"
pvalue_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/pvalues/"

# FUNCTIONS
def perform_t_test(cpg_id):
    if os.path.isfile(cpg_dir+cpg_id+".txt"):
        table = "<table id='pvalues' class='table table-hover'><thead><tr><th>Cancer type</th><th>p value</th></tr></thead><tbody>"
        pvals = ""
        datafile = cpg_dir + cpg_id + ".txt"
        df = pd.read_table(datafile,sep=",",header=0,index_col=None)
        cancer_counts = {}
        for i in df["Acronym"]:
            if i not in cancer_counts:
                cancer_counts[i] = 1
            else:
                cancer_counts[i] += 1
        cancer_to_test = [i for i in cancer_counts if cancer_counts[i]>100 and df[(df["Acronym"]==i) & (df["TumorNormal"]=="Normal")].shape[0]>3]
        cancer_to_test.sort()
        for i in cancer_to_test:
            table += "<tr><td scope='col'>{0}</td>".format(i)
            subdf = df[df["Acronym"]==i]
            t,p = ttest_ind(subdf[subdf["TumorNormal"]=="Tumor"]["Value"], subdf[subdf["TumorNormal"]=="Normal"]["Value"],equal_var=False,nan_policy='omit')
            table += "<td>{0}</td>".format(p)
            table += "</tr>"
            pvals += "{0},{1}\n".format(i,p)
        table += "</tbody></table>"
        with open(pvalue_table_dir + cpg_id + ".html","w") as f:
            f.write(table)
        with open(pvalue_dir + cpg_id + ".txt","w") as f:
            f.write(pvals)

# MAIN
if __name__ == '__main__':
    with open(base_dir + "cpg_list.txt", "r") as f:
        remain_cpgs = [i.strip().split("\t")[0] for i in f.readlines()]
    pool = mp.Pool(num_thread)
    pool.map(perform_t_test,remain_cpgs)