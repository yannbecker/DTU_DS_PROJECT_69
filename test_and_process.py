import json
import gzip
import sys


# --- CONFIGURATION ---

# Fichier des métadonnées (JSON Lines)
METADATA_FILE = "data/arxiv-metadata-oai-snapshot.json" 

# Fichier du graphe original (JSON unique)
GRAPH_FILE = "data/internal-references-pdftotext.json"

# Fichier de sortie pour le graphe nettoyé
# (Utiliser .json.gz est recommandé pour économiser de l'espace)
PROCESSED_GRAPH_FILE = "data/internal-references-processed.json.gz"

# Clés
METADATA_ID_KEY = "id"
GRAPH_SOURCE_ID_KEY = "id" 
GRAPH_REFERENCES_KEY = "references"

# --- FIN DE LA CONFIGURATION ---


def load_metadata_ids(metadata_file, id_key):
    """
    [ÉTAPE 1] Charge tous les ID du fichier de métadonnées dans un set.
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
        sys.exit(1)
        
    print(f"[Étape 1] Terminé. {len(metadata_ids)} ID uniques chargés.")
    return metadata_ids

def check_and_filter_graph(graph_file, metadata_ids, processed_graph_file):
    """
    [ÉTAPE 2 & 3] Vérifie le graphe, crée une version filtrée et l'enregistre.
    """
    print(f"\n[Étape 2] Vérification et filtrage des nœuds depuis {graph_file}...")
    print("   (Mode de lecture: Objet JSON unique)")
    
    try:
        f_open = gzip.open if graph_file.endswith('.gz') else open
        
        graph_data = None
        with f_open(graph_file, 'rt', encoding='utf-8') as f:
            try:
                print("   ... Chargement de l'objet JSON du graphe en mémoire...")
                graph_data = json.load(f) 
            except json.JSONDecodeError as e:
                print(f"ERREUR: Le fichier graphe n'est pas un JSON valide. Erreur: {e}")
                sys.exit(1)
        
        print(f"   ... {len(graph_data):,} articles sources chargés. Vérification et filtrage...")
        
        # --- NOUVEAU : Initialisation des sets de stats et du graphe filtré ---
        missing_ids = set()
        all_graph_ids = set()
        processed_graph = {} # Le nouveau graphe filtré
        
        nodes_checked = 0
        
        # Itération sur le graphe original chargé en mémoire
        for source_id, references in graph_data.items():
            
            # --- 1. Vérification de la source (pour les stats) ---
            all_graph_ids.add(source_id)
            nodes_checked += 1
            source_is_valid = (source_id in metadata_ids)
            
            if not source_is_valid:
                missing_ids.add(source_id)
            
            # --- 2. Vérification des références (pour les stats) et préparation de la liste filtrée ---
            processed_references_list = []
            if references:
                for ref_id in references:
                    all_graph_ids.add(ref_id)
                    nodes_checked += 1
                    ref_is_valid = (ref_id in metadata_ids)
                    
                    if not ref_is_valid:
                        missing_ids.add(ref_id)
                    
                    # On ajoute la réf à la liste SI ET SEULEMENT SI
                    # la source ET la référence sont valides.
                    if source_is_valid and ref_is_valid:
                        processed_references_list.append(ref_id)
            
            # --- 3. Construction du graphe filtré ---
            # On ajoute la source au nouveau graphe SEULEMENT si elle est valide.
            if source_is_valid:
                processed_graph[source_id] = processed_references_list
                
            if nodes_checked % 1000000 == 0 and nodes_checked > 0:
                print(f"   ... {nodes_checked:,} nœuds (vérifications) effectués")

        print(f"[Étape 2] Terminé. {nodes_checked:,} vérifications de nœuds effectuées.")
        
        # --- NOUVEAU : ÉTAPE 3 - ENREGISTREMENT ---
        print(f"\n[Étape 3] Enregistrement du graphe filtré dans {processed_graph_file}...")
        try:
            # Détermine s'il faut utiliser gzip.open ou open standard
            f_save_open = gzip.open if processed_graph_file.endswith('.gz') else open
            
            with f_save_open(processed_graph_file, 'wt', encoding='utf-8') as f_out:
                json.dump(processed_graph, f_out)
                
            print(f"[Étape 3] Terminé. {len(processed_graph)} articles sources valides enregistrés.")
        except Exception as e:
            print(f"ERREUR lors de l'enregistrement du graphe filtré: {e}")
            
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

    # Étape 2 (Vérification) et 3 (Enregistrement)
    missing_ids, all_graph_ids = check_and_filter_graph(
        GRAPH_FILE, 
        metadata_ids, 
        PROCESSED_GRAPH_FILE # Nouvel argument
    )

    # Étape 4: Rapport final
    print("\n--- RAPPORT FINAL (Étape 4) ---")
    
    total_unique_graph_ids = len(all_graph_ids)
    total_missing_ids = len(missing_ids)
    
    if total_unique_graph_ids == 0:
        print("Aucun ID unique n'a été trouvé dans le fichier graphe.")
        total_found_ids = 0
        percentage_found = 0.0
    else:
        total_found_ids = total_unique_graph_ids - total_missing_ids
        percentage_found = (total_found_ids / total_unique_graph_ids) * 100

    print(f"Total des ID uniques dans le graphe *original* : {total_unique_graph_ids:,}")
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