#!/usr/bin/python
"""
"""

# IMPORT
import sys
import pandas as pd
from MySQLdb import Connect
import uuid
import os

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
    with open(acronym_file,"r") as f:
        acronyms = f.read().strip().split(",")
    with open(status_file,"r") as f:
        tumor_normals = f.read().strip().split(",")
    cpg_ids = list(df.index)
    # filename = str(uuid.uuid4())
    output = '/var/www/html/MethylDB/Result/{0}.txt'
    # return_filename = '/MethylDB/Result/{0}.txt'
    # with open(output,"w") as f:
    #     f.write("Acronym,TumorNormal," + ",".join(cpg_ids) + "\n")
    #     for i in range(len(df.columns[2:])):
    #         col = df.columns[2:][i]
    #         values = [str(j) for j in list(df[col])]
    #         line = "{0},{1},".format(acronyms[i],tumor_normals[i]) + ",".join(values)+"\n"
    #         f.write(line)
    for cpg_id in cpg_ids:
        if not os.path.isfile(output.format(cpg_id)):
            with open(output.format(cpg_id),"w") as f:
                f.write("Acronym,TumorNormal,Value\n")
                values = df.loc[cpg_id,df.columns[2:]]
                for i in range(len(acronyms)):
                    line = "{0},{1},{2}\n".format(acronyms[i],tumor_normals[i],str(values[i]))
                    f.write(line)
    return ",".join(cpg_ids)







    return ",".join(cpg_ids) + "," + return_filename




# MAIN
if __name__ == '__main__':
    input = sys.argv[1]
    output = process_data(input)
    print(output)
