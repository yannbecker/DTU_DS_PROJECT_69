import json
import gzip
import sys

# --- À CONFIGURER ---

# Mettez ici le chemin vers votre fichier de métadonnées (le gros fichier JSON)
# Je suppose qu'il s'agit d'un fichier "JSON Lines" (.jsonl ou .json)
# où chaque ligne est un objet JSON séparé.
METADATA_FILE = "data/arxiv-metadata-oai-snapshot.json" 

# Mettez ici le chemin vers votre fichier de graphe (celui de l'Option 1)
# Je suppose qu'il s'agit du fichier .json.gz mentionné dans les dépôts
GRAPH_FILE = "data/internal-references-pdftotext.json"

# Ajustez ces clés si elles sont différentes dans vos fichiers
METADATA_ID_KEY = "id"           # Clé pour l'ID de l'article dans le fichier de métadonnées
GRAPH_SOURCE_ID_KEY = "id"     # Clé pour l'ID de l'article "citant"
GRAPH_REFERENCES_KEY = "references" # Clé pour la liste des articles "cités"

# --- FIN DE LA CONFIGURATION ---


def load_metadata_ids(metadata_file, id_key):
    """
    Charge tous les ID du fichier de métadonnées dans un set.
    """
    print(f"[Étape 1] Chargement des ID depuis {metadata_file}...")
    metadata_ids = set()
    count = 0
    try:
        with open(metadata_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    article = json.loads(line)
                    if id_key in article:
                        metadata_ids.add(article[id_key])
                        count += 1
                    if count % 500000 == 0 and count > 0:
                        print(f"  ... {count} articles chargés")
                except json.JSONDecodeError:
                    print(f"Erreur de décodage JSON sur une ligne, passage.")
                    
    except FileNotFoundError:
        print(f"ERREUR: Le fichier de métadonnées '{metadata_file}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Une erreur est survenue lors de la lecture du fichier de métadonnées: {e}")
        print("S'il ne s'agit pas d'un fichier JSON Lines, le script doit être adapté.")
        sys.exit(1)
        
    print(f"[Étape 1] Terminé. {len(metadata_ids)} ID uniques chargés.")
    return metadata_ids

def check_graph_nodes(graph_file, metadata_ids, source_key, refs_key):
    """
    Vérifie chaque noeud (source et cible) du fichier graphe contre le set d'ID.
    """
    print(f"\n[Étape 2] Vérification des nœuds du graphe depuis {graph_file}...")
    
    # Utilise gzip.open pour lire les fichiers .gz directement
    try:
        f_open = gzip.open if graph_file.endswith('.gz') else open
        
        with f_open(graph_file, 'rt', encoding='utf-8') as f:
            
            missing_ids = set()
            nodes_checked = 0
            
            for line in f:
                try:
                    entry = json.loads(line)
                    
                    # 1. Vérifier le nœud source (l'article citant)
                    source_id = entry.get(source_key)
                    if not source_id:
                        continue # Pas d'ID source, on passe
                        
                    nodes_checked += 1
                    if source_id not in metadata_ids:
                        missing_ids.add(source_id)
                        
                    # 2. Vérifier tous les nœuds cibles (les références)
                    references = entry.get(refs_key, [])
                    for ref_id in references:
                        nodes_checked += 1
                        if ref_id not in metadata_ids:
                            missing_ids.add(ref_id)
                            
                    if nodes_checked % 1000000 == 0:
                        print(f"  ... {nodes_checked:,} nœuds vérifiés")
                        
                except json.JSONDecodeError:
                    print(f"Erreur de décodage JSON sur une ligne du graphe, passage.")
                    
        print(f"[Étape 2] Terminé. {nodes_checked:,} nœuds (sources et cibles) vérifiés.")
        return missing_ids

    except FileNotFoundError:
        print(f"ERREUR: Le fichier graphe '{graph_file}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Une erreur est survenue lors de la lecture du fichier graphe: {e}")
        sys.exit(1)


def main():
    # Étape 1
    metadata_ids = load_metadata_ids(METADATA_FILE, METADATA_ID_KEY)
    
    if not metadata_ids:
        print("Aucun ID n'a été chargé depuis les métadonnées. Arrêt du script.")
        return

    # Étape 2
    missing_ids = check_graph_nodes(
        GRAPH_FILE, 
        metadata_ids, 
        GRAPH_SOURCE_ID_KEY, 
        GRAPH_REFERENCES_KEY
    )

    # Étape 3: Rapport final
    print("\n--- RAPPORT FINAL ---")
    if not missing_ids:
        print("✅ SUCCÈS ! Tous les noeuds (citants et cités) du fichier graphe")
        print(f"ont été trouvés dans le fichier de métadonnées.")
    else:
        print(f"❌ ERREUR : {len(missing_ids)} ID uniques du graphe n'ont PAS été trouvés dans les métadonnées.")
        print("Voici quelques exemples d'ID manquants :")
        for i, missing_id in enumerate(list(missing_ids)[:10]):
            print(f"  - {missing_id}")
        if len(missing_ids) > 10:
            print(f"  ... et {len(missing_ids) - 10} autres.")

if __name__ == "__main__":
    main()