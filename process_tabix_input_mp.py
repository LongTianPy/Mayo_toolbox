#!/usr/bin/python
"""Try using multiple threads to process tabix input
"""

# IMPORT
import multiprocessing as mp
import pandas as pd
import sys
import uuid

# VARIABLES
num_thread = mp.cpu_count()/4
probe_annot_file = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/probe_notnull.txt"
probe_annot = pd.read_table(probe_annot_file, sep='\t', index_col=0, header=0, dtype='string')
outfile = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/mData_chunks/mData_chunks.txt"

# FUNCTIONS
def process_subdf(df):
    probes = list(df.index)
    this_chrom = [str(i) for i in list(probe_annot.loc[probes, 'CHR'].values)]
    pos_list = list(probe_annot.loc[probes, 'MAPINFO'].values)
    this_pos = [int(i) for i in pos_list]
    new = pd.concat([chunk])
    new.insert(loc=0, column='ID', value=list(df.index))
    new.insert(loc=0, column='POS', value=this_pos)
    new.insert(loc=0, column='#CHROM', value=this_chrom)
    new.to_csv(outfile+"."+uuid.uuid4(),header=None,index=False,sep="\t")
    return new


# MAIN
if __name__ == '__main__':
    methyl_file = sys.argv[1]
    pool = mp.Pool(num_thread)
    df_list = []
    methyl_chunks = pd.read_table(methyl_file,sep='\t',index_col=0,header=0,engine='python',chunksize=5000,iterator=True)
    for chunk in methyl_chunks:
        df = pool.apply_async(process_subdf,chunk)
        df_list.append(df)
    pool.close()

