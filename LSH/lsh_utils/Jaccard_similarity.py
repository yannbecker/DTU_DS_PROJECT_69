from lsh_utils.shingle import shingle

def Jaccard_similarity_signatures(input_signature, doc_signature) -> float :
    "Returns an approximation of the jaccard similarity between 2 documents doc_name1 and doc_name2 using signatures"
    sig = doc_signature 
    S = 0
    k = len(sig) # size of signature list
    assert k == len(input_signature), "Signatures are not matching size"
    for i in range(k): # Loop over the signature pairs from the two documents
        if sig[i]==input_signature[i]:
            S+=1
    return S/k

def Jaccard_similarity_shingles(shingles_list_A, shingles_list_B):
    """
    Calculates the Jaccard similarity coefficient between two lists of shingles.

    Args:
        shingles_list_A (list): The list of shingles for the first document.
        shingles_list_B (list): The list of shingles for the second document.

    Returns:
        float: The Jaccard similarity score (0.0 to 1.0).
    """

    # 1. Convert lists to sets for efficient set operations and to ensure uniqueness
    set_A = set(shingles_list_A)
    set_B = set(shingles_list_B)

    # 2. Calculate the size of the intersection (common shingles)
    intersection_size = len(set_A.intersection(set_B))
    
    # Alternatively: intersection_size = len(set_A & set_B)

    # 3. Calculate the size of the union (all unique shingles combined)
    union_size = len(set_A.union(set_B))
    
    # Alternatively: union_size = len(set_A | set_B)

    # 4. Calculate the Jaccard score
    if union_size == 0:
        # Avoid division by zero if both lists are empty
        return 0.0

    jaccard_score = intersection_size / union_size
    return jaccard_score


def Jaccard_similarity(input_text ,article_list, candidate_idx, q):
    return Jaccard_similarity_shingles(
        shingles_list_A= shingle(q = q, text = input_text),
        shingles_list_B= shingle(q = q, text = article_list[candidate_idx]["clean_text"]))