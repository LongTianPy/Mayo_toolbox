#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
from MySQLdb import Connect
import sys
from uuid import uuid4
import zipfile
import os
from os.path import isfile

# VARIABLES
cpg_dir = "/data1/MethylDB/CpG/cpg_result/"
tmp_dir = "/var/www/html/MethylDB/tmp/"
sample_file = "/data1/MethylDB/samples.txt"

# FUNCTIONS
def connect_to_db():
    conn = Connect(host="localhost", user="methyldb",passwd="mayoproject",db="MethylDB")
    c = conn.cursor()
    return conn, c

def find_cpg(chr,start,end):
    conn,c = connect_to_db()
    c.execute("select Probeset_ID from Probeset where CHR={0} and MAPINFO>={1} and MAPINFO<={2}".format(chr,start,end))
    tmp = c.fetchall()
    cpg_ids = [i[0] for i in tmp]
    cpg_ids = [i for i in cpg_ids if isfile(cpg_dir + i + ".txt")]
    return cpg_ids

def merge_dfs(cpg_ids):
    df = pd.read_table(cpg_dir + cpg_ids[0] + ".txt",sep=",",header=0)
    df.columns = ["Acronym","Status",cpg_ids[0]]
    with open(sample_file,"r") as f:
        samples = f.readlines()[0].strip().split("\t")
    df.insert(loc=0,column='Sample',value=samples)
    if len(cpg_ids)>1:
        for cpg_id in cpg_ids[1:]:
            iter_df = pd.read_table(cpg_dir + cpg_id + ".txt",sep=",",header=0)
            df[cpg_id] = iter_df['Value']
    file_name = str(uuid4()) + ".txt"
    df.to_csv(tmp_dir + file_name, sep="\t",index=None)
    zip = file_name + ".zip"
    zipped = zipfile.ZipFile(tmp_dir + zip,"w")
    zipped.write(tmp_dir + file_name,compress_type=zipfile.ZIP_DEFLATED)
    zipped.close()
    os.remove(tmp_dir + file_name)
    return zipfile

# MAIN
if __name__ == '__main__':
    chr = sys.argv[1]
    start = sys.argv[2]
    end = sys.argv[3]
    output = merge_dfs(find_cpg(chr,start,end))
    print(output)
