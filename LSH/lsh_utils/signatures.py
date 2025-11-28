import dis
from lsh_utils.minhash import minhash
from lsh_utils.shingle import shingle
from math import floor

def signatures(doc_list, shingle_size, signature_size):
    "returns the dictionary of the signatures of each document. Key = doc id, Value = signature"
    sig = {} # dictionary of the signatures of each document
    # signature of doc no "id" using minhashing on the shingle_list. Size of the shingles is shingle_size
    n = len(doc_list)
    count = 0
    displayed = {}
    for d in doc_list :
        id = d["id"]
        abstract = d["abstract"]
        sig[id] = minhash(shingle(q=shingle_size, text=abstract), k=signature_size) 
        count += 1
        percentage = 100*count/n
        if floor(percentage) not in displayed :
            displayed[percentage] = None
            print(f"{percentage} % of signatures computed ... \n")
    print("min hashing of the documents complete")
    
    return sig     