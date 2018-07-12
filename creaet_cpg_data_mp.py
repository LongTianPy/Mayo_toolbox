#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import os
import multiprocessing as mp

# VARIABLES
num_thread = mp.cpu_count()
print(num_thread)
cpg_list_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_list.txt"
tabix_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/tabix_result/"
cpg_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"
big_table = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/mData_output.txt.gz"
acronym_file="/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Acronyms.txt"
status_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/Tumor_Normal.txt"


# FUNCTIONS
def query_table(line):
    cpg_id = line[0]
    chromosome = line[1]
    mapinfo = line[2]
    cmd = "tabix {0} {1}:{2}-{2} -h > {3}{4}.txt".format(big_table,chromosome,mapinfo,tabix_dir,cpg_id)
    os.system(cmd)

def process_df(line):
    cpg_id = line[0]
    df = pd.read_table(tabix_dir + cpg_id + ".txt", sep="\t",header=0,index_col=2)
    if  len(df.index)>0:
        df = df.iloc[:,2:]
        with open(acronym_file,"r") as f:
            acronyms = f.read().strip().split(",")
        with open(status_file,"r") as f:
            tumor_normals = f.read().strip().split(",")
        output = cpg_dir + cpg_id + ".txt"
        with open(output,"w") as f:
            f.write("Acronym,TumorNormal,Value\n")
            for i in range(len(df.columns)):
                line = "{0},{1},{2}\n".format(acronyms[i],tumor_normals[i],df.iloc[0,i])
                f.write(line)

def wrap(line):
    cpg_id = line[0]
    output = cpg_dir + cpg_id + ".txt"
    if not os.path.isfile(output):
        query_table(line)
        process_df(line)
    os.remove(tabix_dir + cpg_id + ".txt")

# MAIN
if __name__ == '__main__':
    pool = mp.Pool(num_thread)
    with open(cpg_list_file,"r") as f:
        lines = [i.strip().split("\t") for i in f.readlines()]
    pool.map(wrap,lines)