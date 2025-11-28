from minhash import minhash
from shingle import shingle

def signatures(doc_list, shingle_size, signature_size):
    "returns the dictionary of the signatures of each document. Key = doc id, Value = signature"
    sig = {} # dictionary of the signatures of each document
    # signature of doc no "id" using minhashing on the shingle_list. Size of the shingles is shingle_size
    for d in doc_list :
        id = d["id"]
        abstract = d["abstract"]
        sig[id] = minhash(shingle(q=shingle_size, text=abstract), k=signature_size) 
        #print("document id : ",id," processed")
    print("min hashing of the documents complete")
    
    return sig     