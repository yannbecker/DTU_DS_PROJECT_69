import mmh3

def lsh_band_hash(band, m, lsh_seed) -> int:
    """
    Computes a hash value for a single band (a list of r integers).
    The goal is to map identical bands to the same hash bucket.

    band: A list of r integers representing the signature's portion 
            for a specific band.
    Returns: An integer hash value for the band.
    """
    
    band_string = ",".join(map(str, band)) # Convert the band to a string representation.
    
    hash_value = mmh3.hash(band_string, lsh_seed) # Compute the hash using MurmurHash3.
    
    return abs(hash_value) % m # Ensure non-negative and fit within m buckets


