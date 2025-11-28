from lsh_utils.minhash import minhash
from lsh_utils.shingle import shingle
import numpy as np
from tqdm import tqdm

def signatures(doc_list, shingle_size, signature_size):
    """
    inputs :
        - doc_lists : list of document of the form {'id': ... , 'abstract' : ... }
        - signature_size : size of the signatures
    outputs :
        - sig : signature matrix, every column represent the signature of a document
        - idx_to_sig : dictionnary that matches idexes (columns of sig) withs ids of the documents
    """
    idx_to_id = {} # dictionary of the signatures of each document
    n = len(doc_list)
    sig = np.zeros((signature_size,n))
    # signature of doc no "id" using minhashing on the shingle_list. Size of the shingles is shingle_size
    for i in tqdm(range(n), desc="Computing signatures"):
        id = doc_list[i]["id"]
        abstract = doc_list[i]["abstract"]
        idx_to_id[i] = id
        sig[:,i] = np.array(minhash(shingle(q=shingle_size, text=abstract), k=signature_size))
    print("min hashing of the documents complete")
    return sig, idx_to_id