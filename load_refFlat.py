#!/usr/bin/python
"""
"""

# IMPORT
from MySQLdb import Connect

# FUNCTIONS
def connect_to_db():
    conn = Connect(host="localhost", user="methyldb",passwd="mayoproject",db="MethylDB")
    c = conn.cursor()
    return conn, c



# MAIN
if __name__ == '__main__':
    f = open("/home/long-lamp-username/MethylDB/refFlat.txt","r")
    lines = [i.strip().split("\t") for i in f.readlines()]
    f.close()
    conn,c = connect_to_db()
    for i in lines:
        sql = "insert into hg19 (geneName,name,chrom,strand,txStart,TxEnd,cdsStart,cdsEnd,exonCount,exonStarts,exonEnds) " \
              "values ('{0}','{1}','{2}','{3}',{4},{5},{6},{7},{8},'{9}','{10}')".format(i[0],i[1],i[2],i[3],i[4],i[5],i[6],
                                                                                  i[7],i[8],i[9],i[10])
        c.execute(sql)
        conn.commit()
