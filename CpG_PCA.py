#!/usr/bin/python
"""
"""

# IMPORT
import pandas as pd
import sklearn
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from os import listdir
import multiprocessing as mp

# VARIABLES
num_threads = 32
base_dir = "/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/filtered_data/"

# FUNCTIONS
def pca(file):
    cancer_type = file.split("_")[0]
    df = pd.read_table(base_dir + file,sep="\t",header=0,index_col=0)
    data = df.iloc[:,3:]
    pca = PCA(n_components=2)
    X_r = pca.fit(data).transform(data)
    colors = ['navy', 'darkorange']
    status = ["Tumor", "Normal"]
    lw = 2
    plt.figure()
    for color, i, target_name in zip(colors, ['Tumor', "Normal"], status):
        plt.scatter(X_r[df['Status'] == i, 0], X_r[df['Status'] == i, 1], color=color, alpha=.8, lw=lw,
                    label=target_name)
    plt.legend(loc='best', shadow=False, scatterpoints=1)
    plt.title('PCA of ' + cancer_type + " data")
    plt.savefig(base_dir + cancer_type + '_pca.pdf')
    plt.clf()


# MAIN
if __name__ == '__main__':
    datafiles = [file for file in listdir(base_dir) if file.endswith(".txt")]
    pool=mp.Pool(num_threads)
    pool.map(pca,datafiles)