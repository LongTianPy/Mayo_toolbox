#!/usr/bin/python
"""
"""

# IMPORT
import numpy as np
from scipy.stats import ttest_ind
import pandas as pd
import multiprocessing as mp
import os
from scipy.cluster.hierarchy import dendrogram, linkage
import scipy.cluster.hierarchy as hc
from scipy.spatial.distance import pdist

# VARIABLES
num_thread = mp.cpu_count()/2
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/"
cpg_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"
cpg_list_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_list.txt"
samples_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/samples.txt"
acronym_file="/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Acronyms.txt"
status_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Tumor_Normal.txt"

# FUNCTIONS
def perform_t_test(cpg_id):
    cpg_file = cpg_dir + cpg_id + ".txt"
    if os.path.isfile(cpg_file):
        df = pd.read_table(cpg_file,sep=",",header=0)
        t, p = ttest_ind(df[df["TumorNormal"]=="Tumor"]["Value"],df[df["TumorNormal"]=="Normal"]["Value"],equal_var=False)
        if p < 0.05:
            return cpg_id
        else:
            return 0
    else:
        return 0

def concat_df(new_df,cpg_id):
    individual_df = pd.read_table(cpg_dir + cpg_id + ".txt", sep=",",header=0)
    new_df[cpg_id] = individual_df["Value"]


# MAIN
if __name__ == '__main__':
    # pool = mp.Pool(num_thread)
    # with open(cpg_list_file, "r") as f:
    #     lines = [i.strip().split("\t")[0] for i in f.readlines()]
    # results = [pool.apply_async(perform_t_test,(cpg,)) for cpg in lines]
    # get_result = [r.get() for r in results]
    # with open(base_dir + "remaining_cpg.txt","w") as f:
    #     for i in get_result:
    #         if i != 0:
    #             f.write(i + "\n")
    with open(base_dir + "cpg_list.txt", "r") as f:
        remain_cpgs = [i.strip() for i in f.readlines()]
    with open(samples_file,"r") as f:
        samples = f.readlines()[0].strip().split("\t")
    with open(acronym_file, "r") as f:
        acronyms = f.read().strip().split(",")
    with open(status_file, "r") as f:
        tumor_normals = f.read().strip().split(",")
    # new_df = pd.DataFrame()
    # new_df["Sample"] = samples
    # new_df["Acronyms"] = acronyms
    # new_df["Status"] = tumor_normals
    # for cpg in remain_cpgs:
    #     concat_df(new_df,cpg)
    # new_df.to_csv(base_dir + "filtered_mData_w_meta.txt",sep="\t",)
    # data_table = new_df.iloc[:,3:]
    # data_table = data_table.T
    # data_table.columns = samples
    # data_table.to_csv(base_dir + "filtered_mData.txt",sep="\t",header=0,index_label="CpG_ID")
    # dists =pdist(data_table)
    # ward_link = linkage(data_table,'ward')
    # cut = hc.cut_tree(ward_link,2)
    df = pd.read_table(base_dir + "filtered_mData_w_meta.txt",sep="\t", index_col=0,header=None)
    colnames = ["Sample","Acronym","Status"] + remain_cpgs
    df.columns = colnames
    samples = list(df['Sample'])
    acronyms = list(df['Acronym'])
    status = list(df['Status'])
    acronym_count = {}
    for i in acronyms:
        if i not in acronym_count:
            acronym_count[i] = 1
        else:
            acronym_count[i] += 1
    acronyms_to_study = [i for i in acronym_count if acronym_count[i]>100]
    for acro in acronyms_to_study:
        sub_df = df[df["Acronym"]==acro]
        normal_sub = sub_df[sub_df["Status"]=='Normal']
        if normal_sub.shape[0]>0:
            sub_df.to_csv(base_dir + acro + "_filtered.txt",sep="\t")










