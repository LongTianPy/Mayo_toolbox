#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import sys

# FUNCTIONS


# MAIN
if __name__ == '__main__':
    probe_annot_file = sys.argv[1]
    methyl_file = sys.argv[2]
    probe_annot = pd.read_table(probe_annot_file,sep='\t',index_col=0,header=0,engine='python')
    methyl_chunks = pd.read_table(methyl_file,sep='\t',index_col=0,header=0,engine='python',chunksize=500,iterator=True)
    chrom = []
    pos = []
    for chunk in methyl_chunks:
        probes = list(chunk.index)
        this_chrom = list(probe_annot.loc[probes,'CHR'].values)
        this_pos = list(probe_annot.loc[probes,'MAPINFO'].values)
        chrom = chrom + this_chrom
        pos = pos + this_pos
    del methyl_chunks
    methyl_chunks = pd.read_table(methyl_file,sep='\t',index_col=0,header=0,engine='python',chunksize=500,iterator=True)
    methyl_df = pd.concat(methyl_chunks)
    methyl_df["CHROM"] = chrom
    methyl_df["POS"] = pos
    methyl_df.to_csv('processed_mData.txt')