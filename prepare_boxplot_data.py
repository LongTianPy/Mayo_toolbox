#!/usr/bin/python
"""
"""

# IMPORT
import sys
import pandas as pd
from MySQLdb import Connect
import os
import uuid


acronym_file="/home/long-lamp-username/MethylDB/Acronyms.txt"
status_file = "/home/long-lamp-username/MethylDB/Tumor_Normal.txt"
# FUNCTIONS
def connect_to_db():
    conn = Connect(host="localhost", user="methyldb",passwd="mayoproject",db="MethylDB")
    c = conn.cursor()
    return conn, c

def find_sampleType(sample,c):
    c.execute("select acronym,TumorNormal from Patient where Sample_ID='{0}'".format(sample))
    tmp = c.fetchone()
    return tmp


def process_data(input):
    # conn,c=connect_to_db()
    df = pd.read_table(input,sep="\t",header=0,index_col=2)
    cpg = list(df.index)[0]
    df = df.iloc[0,2:]
    with open(acronym_file,"r") as f:
        acronyms = f.read().strip().split(",")
    with open(status_file,"r") as f:
        tumor_normals = f.read().strip().split(",")
    filename = cpg
    output = '/data1/MethylDB/CpG/'+str(filename)+'.txt'
    return_value = '/MethylDB/Result/'+filename+".txt"
    if not os.path.isfile(output):
        with open(output,"w") as f:
            f.write("Patient,Acronym,TumorNormal,Value\n")
            for i in range(len(df.index)):
                line = "{0},{1},{2},{3}\n".format(str(df.index[i]),acronyms[i],tumor_normals[i],df[df.index[i]])
                f.write(line)
    return return_value

# MAIN
if __name__ == '__main__':
    input = sys.argv[1]
    output = process_data(input)
    print(output)