#!/usr/bin/python
"""Create each individual cpg a file
"""

# IMPORT
import sys
import pandas as pd
from MySQLdb import Connect
import uuid
import os

acronym_file="/home/long-lamp-username/MethylDB/Acronyms.txt"
status_file = "/home/long-lamp-username/MethylDB/Tumor_Normal.txt"
big_table = "/home/long-lamp-username/MethylDB/mData_output.txt.gz"
base_dir = "/data1/MethylDB/"

# FUNCTIONS
def connect_to_db():
    conn = Connect(host="localhost", user="methyldb",passwd="mayoproject",db="MethylDB")
    c = conn.cursor()
    return conn, c

def get_all_cpg(c):
    c.execute("select Probeset_ID from Probeset")
    tmp = c.fetchall()
    cpgs = [i[0] for i in tmp]
    return cpgs

def query_table(cpg_id,c):
    c.execute("select CHR,MAPINFO from Probeset where Probeset_ID='{0}'".format(cpg_id))
    tmp = c.fetchone()
    chromosome = tmp[0]
    mapinfo = tmp[1]
    cmd = "tabix {0} {1}:{2}-{3} -h > {4}output.txt".format(big_table,chromosome,mapinfo,mapinfo,base_dir)
    os.system(cmd)

def process_df(cpg_id):
    df = pd.read_table(base_dir+"output.txt",sep="\t",header=0,index_col=2)
    df = df.iloc[:,2:]
    if len(df.index)==1:
        with open(acronym_file,"r") as f:
            acronyms = f.read().strip().split(",")
        with open(status_file,"r") as f:
            tumor_normals = f.read().strip().split(",")
        output = base_dir + "CpG/" + cpg_id + ".txt"
        if not os.path.isfile(output):
            with open(output,"w") as f:
                f.write("Acronym,TumorNormal,Value\n")
                for i in range(len(df.columns)):
                    line = "{0},{1},{2}\n".format(acronyms[i],tumor_normals[i],df.iloc[0,i])
                    f.write(line)

# MAIN
if __name__ == '__main__':
    conn,c=connect_to_db()
    cpgs = get_all_cpg(c)
    for cpg in cpgs:
        if not os.path.isfile(base_dir + "CpG/" + cpg + ".txt"):
            query_table(cpg,c)
            process_df(cpg)
    c.close()
    conn.close()