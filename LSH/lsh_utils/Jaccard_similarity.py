# Jaccard similarity

def Jaccard_similarity(input_signature, doc_name, dic_signatures) -> float :
    "Returns an approximation of the jaccard similarity between 2 documents doc_name1 and doc_name2 using signatures"
    sig = dic_signatures[doc_name] 
    S = 0
    k = len(sig) # size of signature list
    assert k == len(input_signature), "Signatures are not matching size"
    for i in range(k): # Loop over the signature pairs from the two documents
        if sig[i]==input_signature[i]:
            S+=1
    return S/k