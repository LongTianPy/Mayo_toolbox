#!/usr/bin/python
"""
"""

# IMPORT
import sys
from MySQLdb import Connect


# FUNCTIONS
def connect_to_db():
    conn = Connect(host="localhost", user="methyldb",passwd="mayoproject",db="MethylDB")
    c = conn.cursor()
    return conn, c

def parse_gff(gff):
    f = open(gff,"r")
    lines = [i.strip().split("\t") for i in f.readlines()[9:-1] if not i.startswith("#")]
    f.close()
    new_line = []
    CHR=''
    for i in lines:
        if i[2] == 'region':
            CHR = i[3]
        elif i[2] == 'gene':
            start = i[3]
            end = i[4]
            info = i[8].split(";")
            for each in info:
                if each.startswith("Name="):
                    gene = each[5:]
            new_line.append([CHR,start,end,gene])
        else:
            continue
    with open('filtered_hg19.txt','w') as f:
        for i in new_line:
            f.write("\t".join(i) + "\n")
    return new_line


# MAIN
if __name__ == '__main__':
    gff = sys.argv[1]
    lines = parse_gff(gff)