#!/usr/bin/python
"""
"""

# IMPORT
import sys
import pandas as pd
from os.path import isfile

# VARIABLES
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/filtered_data/"
cpg_table = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/filtered_data/CpG_ID_by_island.txt"
cpg_result = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/cpg_result/"

# FUNCTIONS
def create_island_id(cpg_table):
    df = pd.read_table(cpg_table,sep="\t",header=0,index_col=None,dtype='str')
    to_remove = []
    for i in df.index:
        cpg_id = df.loc[i,"Probeset_ID"]
        if not isfile(cpg_result+cpg_id+".txt"):
            to_remove.append(i)
    df.drop(df.index[to_remove])
    cpg_islands = {}
    for i in df.index:
        cpg = df.loc[i,"Probeset_ID"]
        island_name = df.loc[i,"UCSC_CpG_Islands_Name"]
        island_relation = df.loc[i,"Relation_to_UCSC_CpG_Island"]
        if island_name not in cpg_islands:
            cpg_islands[island_name] = {island_relation:[cpg]}
        else:
            if island_relation not in cpg_islands[island_name]:
                cpg_islands[island_name][island_relation] = [cpg]
            else:
                cpg_islands[island_name][island_relation].append(cpg)
    island_count = 0
    island_id_list = []
    island_name_list = []
    island_relation_list = []
    island_members_list = []
    new_df = pd.DataFrame()
    for i in cpg_islands:
        for j in cpg_islands[i]:
            island_id = "Island_{0}".format(island_count)
            island_count += 1
            island_name = i
            island_relation = j
            island_members = ",".join(cpg_islands[i][j])
            island_id_list.append(island_id)
            island_name_list.append(island_name)
            island_relation_list.append(island_relation)
            island_members_list.append(island_members)
    new_df["Island_ID"] = island_id_list
    new_df["Island_Name"] = island_name_list
    new_df["Island_Relation"] = island_relation_list
    new_df["Island_Members"] = island_members_list
    new_df.to_csv(base_dir+"Island_meta.txt",sep="\t")
    return new_df

def reorganize_data(datafile):
    if isfile(base_dir+"Island_meta.txt"):
        group_df = pd.read_table(base_dir+"Island_meta.txt",sep="\t",header=0)
    else:
        group_df = create_island_id(cpg_table)
    df = pd.read_table(datafile,sep="\t",header=0,index_col=1)
    new_df = pd.DataFrame(0,index=df.index,columns=group_df["Island_ID"])
    for idx in new_df.index:
        for each_island_id in group_df["Island_ID"]:
            sub_df = df.loc[idx,group_df.loc[each_island_id,"Island_Members"].split(",")]
            ave = sub_df.mean(numeric_only=True)
            new_df.loc[idx,each_island_id]=ave
    new_df.to_csv(base_dir + datafile + ".reorganized",sep="\t")

# MAIN
if __name__ == '__main__':
    reorganize_data(base_dir+sys.argv[1])