punctuation = ['.',',',';',':','"']



def string_modulo_q(q, caracter_list, beginning_index):
    """takes a caracter list and a beginning index and returns a string composed of 
     the caracters in order beginning at the beginning index """
    assert q == len(caracter_list)

    string=""

    for i in range(q):
        string += caracter_list[(beginning_index+i)%q]

    return string




def shingle(q, text, punctuation_list = punctuation):
    "Returns the set of q-shingles from the original text"
    n = len(text)
    assert(n>q), "Text too short or shingle too long"
    assert q>=2, "Shingle size must be > 1 caracter "

    ind = 0 # index of the first caracter
    q_list = ["" for _ in range(q)]
    S = []
    ind_modified = 0

    for c in text:
        if (not (c in punctuation_list)) & (c != ' ') : # We ignore punctuation
            
            # Update q_string with another caracter
            q_list[ind] = c
            # Add one to the beginning index
            new_ind = (ind+1)%q
            ind = new_ind
            ind_modified += 1
            # New shingle added to the list
            S.append(string_modulo_q(q = q, caracter_list = q_list, beginning_index = ind))
    return S[q-1:]
            
        

#print(shingle(5,"Bonjour je m'appelle thibaut. J'ai 23 ans et je suis étudiant à DTU."))