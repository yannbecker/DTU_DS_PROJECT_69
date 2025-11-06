import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity
import json

import graph_creation

def community_detection():
    # Corrected typo: 'commutinity' -> 'community'
    results = [] 
    G = graph_creation.graph_creation()


    # Louvain community detection
    communities = louvain_communities(G, seed=42)
    print(communities)
    print(len(communities))
    print(modularity(G, communities))

    for comm in communities:
        # Identify the node with the highest degree as the representative
        representative_article = sorted(comm, key=lambda x: G.in_degree(x), reverse=True)[0]
        results.append({
            'representative_node' : representative_article,
            # Convert set to list for JSON serialization
            'community' : list(comm) 
        })

    with open('data/processed/communities.json', 'w') as outfile:
        # Use indent for better JSON readability
        json.dump(results, outfile, indent=4) 

if __name__ == '__main__':
    community_detection()