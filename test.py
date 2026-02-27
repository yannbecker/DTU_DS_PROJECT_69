import json
import gzip
import sys


# Mettez ici le chemin vers votre fichier de métadonnées (le gros fichier JSON)
METADATA_FILE = "data/arxiv-metadata-oai-snapshot.json" 

# Mettez ici le chemin vers votre fichier de graphe
GRAPH_FILE = "data/internal-references-processed.json.gz"

# Ajustez ces clés si elles sont différentes dans vos fichiers
METADATA_ID_KEY = "id"
GRAPH_SOURCE_ID_KEY = "id" 
GRAPH_REFERENCES_KEY = "references"

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
                        print(f"   ... {count} articles chargés")
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
    Renvoie les ID manquants ET tous les ID uniques trouvés dans le graphe.
    """
    print(f"\n[Étape 2] Vérification des nœuds du graphe depuis {graph_file}...")
    print("   (Mode de lecture: Objet JSON unique)")
    
    try:
        f_open = gzip.open if graph_file.endswith('.gz') else open
        
        with f_open(graph_file, 'rt', encoding='utf-8') as f:
            
            missing_ids = set()
            all_graph_ids = set() # *** NOUVEAU: Pour compter tous les ID uniques ***
            nodes_checked = 0
            
            try:
                print("   ... Chargement de l'objet JSON du graphe en mémoire...")
                graph_data = json.load(f) 
            except json.JSONDecodeError as e:
                print(f"ERREUR: Le fichier graphe n'est pas un JSON valide. Erreur: {e}")
                sys.exit(1)
            
            print(f"   ... {len(graph_data):,} articles sources chargés. Vérification des nœuds...")

            for source_id, references in graph_data.items():
                
                # Ajouter la source au set total
                all_graph_ids.add(source_id)
                
                # 1. Vérifier le nœud source
                nodes_checked += 1
                if source_id not in metadata_ids:
                    missing_ids.add(source_id)
                    
                # 2. Vérifier tous les nœuds cibles
                if references:
                    for ref_id in references:
                        # Ajouter la référence au set total
                        all_graph_ids.add(ref_id)
                        
                        nodes_checked += 1
                        if ref_id not in metadata_ids:
                            missing_ids.add(ref_id)
                            
                if nodes_checked % 1000000 == 0 and nodes_checked > 0:
                    print(f"   ... {nodes_checked:,} nœuds (vérifications) effectués")
            
            
        print(f"[Étape 2] Terminé. {nodes_checked:,} vérifications de nœuds effectuées.")
        # *** MODIFIÉ: Renvoie les deux sets ***
        return missing_ids, all_graph_ids

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
    # *** MODIFIÉ: Récupère les deux sets ***
    missing_ids, all_graph_ids = check_graph_nodes(
        GRAPH_FILE, 
        metadata_ids, 
        GRAPH_SOURCE_ID_KEY, 
        GRAPH_REFERENCES_KEY
    )

    # Étape 3: Rapport final
    # *** MODIFIÉ: Ajout des calculs et du pourcentage ***
    print("\n--- RAPPORT FINAL ---")
    
    total_unique_graph_ids = len(all_graph_ids)
    total_missing_ids = len(missing_ids)
    total_found_ids = total_unique_graph_ids - total_missing_ids
    
    if total_unique_graph_ids == 0:
        print("Aucun ID unique n'a été trouvé dans le fichier graphe.")
        percentage_found = 0.0
    else:
        # Calcul du pourcentage
        percentage_found = (total_found_ids / total_unique_graph_ids) * 100

    print(f"Total des ID uniques dans le graphe : {total_unique_graph_ids:,}")
    print(f"Total des ID trouvés (dans metadata) : {total_found_ids:,}")
    print(f"Total des ID manquants (non trouvés) : {total_missing_ids:,}")
    print(f"")
    print(f"📊 Pourcentage d'ID du graphe présents dans les métadonnées : {percentage_found:.2f}%")
    print(f"")
    
    if not missing_ids:
        print("✅ SUCCÈS ! Tous les noeuds (citants et cités) du fichier graphe")
        print(f"ont été trouvés dans le fichier de métadonnées.")
    else:
        print(f"❌ {total_missing_ids} ID uniques du graphe n'ont PAS été trouvés dans les métadonnées.")
        print("Voici quelques exemples d'ID manquants :")
        for i, missing_id in enumerate(list(missing_ids)[:10]):
            print(f"   - {missing_id}")
        if len(missing_ids) > 10:
            print(f"   ... et {len(missing_ids) - 10} autres.")

if __name__ == "__main__":
    main()