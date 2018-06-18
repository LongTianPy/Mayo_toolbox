#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import sys
import multiprocessing as mp

# FUNCTIONS
def cancer2patient(c):
    sub_df = patient[patient['acronym']==c]
    return list(sub_df.index)

# MAIN
if __name__ == '__main__':
    df_reader = pd.read_table(sys.argv[1],sep="\t",index_col=0,header=0,engine='python',chunksize=1000,iterator=True).T
    patient = pd.read_table(sys.argv[2],sep="\t",index_col=0,header=0)
    pool = mp.Pool(16)
    cancer_type_total = list(set(list(patient['acronym'])))
    cancer_genderless = []
    for each_type in cancer_type_total:
        sub_df = patient[patient['acronym'] == each_type]
        if sub_df.shape[0] > 100:
            if sub_df[sub_df['gender'] == 'MALE'].shape[0] != 0 and sub_df[sub_df['gender'] == 'FEMALE'].shape[0] != 0:
                cancer_genderless.append(each_type)
    cancer_genderless
    patient_genderless = []
    for i in cancer_genderless:
        patient_genderless += cancer2patient(i)
file_number = 0
for chunk in df_reader:
    chunk.transpose().to_csv("chunks/chunk_{0}.txt".format(file_number),sep="\t")
    file_number += 1

