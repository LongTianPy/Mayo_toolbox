#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import sys
import os

# FUNCTIONS


# MAIN
if __name__ == '__main__':
    probe_annot_file = sys.argv[1]
    methyl_file = sys.argv[2]
    outfile = sys.argv[3]
    probe_annot = pd.read_table(probe_annot_file,sep='\t',index_col=0,header=0,dtype='string')
    methyl_chunks = pd.read_table(methyl_file,sep='\t',index_col=0,header=0,engine='python',chunksize=500,iterator=True)
    chrom = []
    pos = []
    for chunk in methyl_chunks:
        probes = list(chunk.index)
        this_chrom = [str(i) for i in list(probe_annot.loc[probes,'CHR'].values)]
        this_pos = [int(i) for i in list(probe_annot.loc[probes,'MAPINFO'].values)]
        chrom = chrom + this_chrom
        pos = pos + this_pos
    del methyl_chunks
    methyl_chunks = pd.read_table(methyl_file,sep='\t',index_col=0,header=0,engine='python',chunksize=500,iterator=True)
    methyl_df = pd.concat(methyl_chunks)
    methyl_df["CHROM"] = chrom
    methyl_df["POS"] = pos
    cols = list(methyl_df.columns)
    idx_chr = cols.index("CHROM") + 1
    idx_pos = cols.index("POS") + 1
    methyl_df.index.name = "#ID"
    methyl_df.sort_values(by=['CHROM','POS'])
    methyl_df.to_csv(outfile,sep="\t")
    # cmd = "(head -n 1 {0} && tail -n +2 {0} | sort -k {1} -k {2}) > sorted_{0}".format(outfile,idx_chr,idx_pos)
    # os.system(cmd)
    os.system("bgzip -c -f {0} > {0}.gz".format(outfile))