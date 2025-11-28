import json
from typing import Dict, Any

import os
from pathlib import Path

from lsh_utils.signatures import signatures
from lsh_utils.Bucket_hashing import lsh_band_hash
from lsh_utils.Jaccard_similarity import Jaccard_similarity

# Preprocessing the dataset to keep only the relevant information

def preprocess_lsh(dataset_path):
    """
    Input : 
        dataset_path : path of the json dataset of articles 
    Output :
        article_list : list of dictionnaries of the form {'id': ..., 'abstract': ...}  
    """
    try : 
        with open(dataset_path, 'r', encoding='utf-8') as f:
            data: Dict[str, Any] = json.load(f)
        print(f"Data succesfully loaded")
        
        article_list = [{'id': article['id'], 'abstract' : article['abstract']} for article in data['articles']] 
        return article_list

    #data = {'articles' : [d1 = {'id' : ..., 'authors' : ...,'abstract': ..., 'clean_text' : ... , 'categories' : ... , 'refs' :  ... } , d2, ...]}

    except Exception as e:
        print(f"Error in loading of the dataset : {e}")

# Implementing lsh function         

def lsh(input,article_list, shingle_size, nb_band, band_size):
    """
    Inputs :
        input : input text from which we want to obtain sources
        article_list : list of arcticles i.e. dictionnaries of the format {'id': ..., 'abstract': ...}
        shingle_size : size of the shingle decomposition on which the minhashing is computed
        nb_band : number of horizontal bands in the signature matrix 
        band_size : number of rows per band in the signature matrix
        signature_size : size of the signatures of the documents obtained from minhashing of
                        the shingle size with signature_size different seeds. 
                        signature_size = nb_band*band_size

    Process :
        - Shingle all documents from the dataset and compute a signature for every document
          using minhashing (signatures function)
        - Find the documents that are most likely to be similar to input using LSH method
        - Compute the actual similarity between input and these document to eliminate false positives
    
    Outputs :
        - Most_similar : list of the most similar documents
        - Scores : list of Jaccard_similarities between input and documents 
    
    """

    # 1. Compute the a signature for every document

    print("Computing the signature of every document in the dataset ...")
    k = band_size*nb_band # Signature size
    # Compute the signature of every document 
    dic_signatures = signatures(
        doc_list = article_list,
        shingle_size = shingle_size,
        signature_size = k)
    print("Signatures done")
    # Compute signature of input
    input_signature = signatures([{'id':'input', 'abstract':input}],
                                 shingle_size = shingle_size,
                                 signature_size = k)['input']
    #print("Input signature done")
    
    # 2. Find the documents that are most likely to be similar to input using LSH method

    print("Performing LSH to find similar candidates ...")
    similar_candidates = {}
    n = len(dic_signatures)  # number articles in the dataset
    m = 2*n # number of buckets

    for band_nb in range(nb_band):
        input_hash = lsh_band_hash(
            band = input_signature[band_nb*band_size:(band_nb+1)*band_size],
            m = m,
            lsh_seed = band_nb
        )
        for doc_id in dic_signatures:
            doc_hash = lsh_band_hash(
                band = dic_signatures[doc_id][band_nb*band_size:(band_nb+1)*band_size],
                m = m,
                lsh_seed = band_nb
                )
            if doc_hash == input_hash :
                if doc_id in similar_candidates :
                    similar_candidates[doc_id] += 1
                else :
                    similar_candidates[doc_id] = 1
        percentage = band_nb+1 / n
        print(f"{percentage} % of signatures computed ... \n")
    print("LSH successfully performed to find similar candidates")

    # 3. Compute the actual similarity between input and these documents

    print("Calculation of the actual similarities ...")
    Ordered_similar_candidates = similar_candidates.keys()
    Ordered_similarities = []
    for doc_id in similar_candidates :
        j = Jaccard_similarity(input_signature = input_signature, 
                               doc_name = doc_id, 
                               dic_signatures = dic_signatures)
        Ordered_similarities.append(j)
    Most_similar = zip(Ordered_similar_candidates,Ordered_similarities)
    Most_similar = sorted(Most_similar, key = lambda pair:pair[1], reverse = True)
    Scores = [ p[1] for p in Most_similar]
    Most_similar = [p[0] for p in Most_similar]
    
    return Most_similar, Scores

if __name__ == "__main__" :

    json_path = 'DTU_DS_PROJECT_69/data/processed/filtered_articles_Nmostcited.json'

    data = preprocess_lsh(dataset_path = json_path)
    print("preprocessing done")
    #print("keys : ", data[0].keys(), '\n', 'length : ',len(data), '\n', "Ids : ", [a['id'] for a in data][:10], '\n', "first abstract : ", data[0]["abstract"][:500] )

    Most_similar, Scores = lsh(
        input = data[0]['abstract'],
        article_list = data,
        shingle_size = 7,
        nb_band = 10,
        band_size = 10,
        )
    
    print("Most similar documents : ", Most_similar, '\n', "Jaccard similarity scores : ", Scores)


    #"hello my name is Bob and I love data science, deep learning and random forests"