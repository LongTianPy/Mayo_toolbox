#!/usr/bin/python
"""
"""

# IMPORT
import sys
import pandas as pd
from MySQLdb import Connect
import uuid

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
    conn,c=connect_to_db()
    df = pd.read_table(input,sep="\t",header=0,index_col=2)
    df = df.iloc[0,2:]
    acronyms = []
    tumor_normals = []
    for i in df.index:
        tmp = find_sampleType(i,c)
        acronyms.append(tmp[0])
        tumor_normals.append(tmp[1])
    filename = str(uuid.uuid4())
    output = '/var/www/html/MethylDB/Result/'+str(filename)+'.txt'
    return_value = 'MethylDB/Result/'+filename+".txt"
    with open(output,"w") as f:
        f.write("Patient\tAcronym\tTumorNormal\tValue\n")
        for i in range(len(df.index)):
            line = "{0}\t{1}\t{2}\t{3}\n".format(str(df.index[i]),acronyms[i],tumor_normals[i],df[df.index[i]])
            f.write(line)
    return

# MAIN
if __name__ == '__main__':
    input = sys.argv[1]
    output = process_data(input)
    print(output)