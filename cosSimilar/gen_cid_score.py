# -*-coding:utf8 -*-
import sys
import numpy as np


def load_only():
    f = open('./cid3_name.txt')
    cid3 = list()
    cid3_index = dict()
    for line in f:
        terms = line.strip().split('\t')
        if int(terms[0])-1 not in cid3_index:
            cid3_index[int(terms[0]) -1] = terms[1]
        cid3.append(terms[1])
    return cid3, cid3_index

def score(embfile, savefile, top):
    fs = open(savefile,'w')
    cid3, cid3_index = load_only()
    vectors = np.loadtxt(embfile)
    vec = np.array(vectors)
    similar = np.dot(vec, vec.T)
    diag = np.diag(similar)
    inv_diag = 1 / diag
    # isinf to 0
    inv_diag[np.isinf(inv_diag)] = 0.0
    inv_diag = np.sqrt(inv_diag)
    
    # cosine similar
    cosine_matric = similar * inv_diag
    cosine_matric = cosine_matric.T * inv_diag
    
    for i,x in enumerate(cosine_matric):
        x_sorted = sorted(x, reverse=True)
        index_sorted = np.argsort(-x)
        final = list()
        for j,(s,idx) in enumerate(zip(x_sorted,index_sorted)):
            if j<int(top):
                final.append(cid3_index[idx]+':'+str(s))
        fs.write(cid3[i]+'\t'+','.join(final)+'\n')
 

if __name__=='__main__':
    embfile, savefile, top= sys.argv[1:]
    score(embfile, savefile, top)
