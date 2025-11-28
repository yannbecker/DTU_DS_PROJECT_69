from lsh_utils.minhash import minhash
from lsh_utils.shingle import shingle
from tqdm import tqdm

def signatures(doc_list, shingle_size, signature_size):
    "returns the dictionary of the signatures of each document. Key = doc id, Value = signature"
    sig = {} # dictionary of the signatures of each document
    # signature of doc no "id" using minhashing on the shingle_list. Size of the shingles is shingle_size
    for d in tqdm(doc_list, desc="Computing signatures"):
        id = d["id"]
        abstract = d["abstract"]
        sig[id] = minhash(shingle(q=shingle_size, text=abstract), k=signature_size) 
    print("min hashing of the documents complete")
    return sig 