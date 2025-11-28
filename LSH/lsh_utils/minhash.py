from shingle import shingle
from mmh3 import hash 



# hashes a list of strings
def listhash(l,seed):
	val = 0
	for e in l:
		val = val ^ hash(e, seed)
	return val 

# Minhash function
def minhash(shingle_list, k):
    "returns a list of k different minhashes of the shingle list"
    return [min(listhash(s, seed) for s in shingle_list) for seed in range(k)]


if __name__ == '__main__' :

	test_text = "Bonjour je m'appelle thibaut. J'ai 23 ans et je suis étudiant à DTU."
	test_shingle_list = shingle(3,test_text,punctuation_list=['.']) 

	test_hashing_seed = 0
	print(minhash(test_shingle_list, 4))
