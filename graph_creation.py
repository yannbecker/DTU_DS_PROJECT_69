import networkx as nx
import json

def graph_creation(json_path='data/processed/articles.json'):
    G = nx.Graph()
    with open(json_path) as json_file:
        data = json.load(json_file)
    for article in data.get('articles'):
        G.add_node(article.get('id'))
        for link in article.get('refs'):
            G.add_edge(article.get('id'), link)
    return G
