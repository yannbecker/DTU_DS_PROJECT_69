import json
import gzip
import sys
import os # Ajouté pour créer le dossier de sortie

# --- À CONFIGURER ---

# Fichiers d'entrée (basés sur votre script)
METADATA_FILE = "data/arxiv-metadata-oai-snapshot.json" 
GRAPH_FILE = "data/internal-references-pdftotext.json"

# Fichier de sortie (selon votre demande)
OUTPUT_FILE = "data/processed/articles.json"

# --- Clés des fichiers (ajustez si nécessaire) ---

# Clés pour le fichier de métadonnées
METADATA_ID_KEY = "id"
METADATA_TITLE_KEY = "title"       # À VÉRIFIER DANS VOTRE JSON
METADATA_ABSTRACT_KEY = "abstract" # À VÉRIFIER DANS VOTRE JSON

# Clés pour le fichier graphe
GRAPH_SOURCE_ID_KEY = "id"
GRAPH_REFERENCES_KEY = "references"

# --- FIN DE LA CONFIGURATION ---


def build_processed_json(metadata_path, graph_path, output_path,
                         meta_id_key, meta_title_key, meta_abstract_key,
                         graph_id_key, graph_refs_key):
    """
    Construit le fichier JSON combiné 'articles.json' à partir des métadonnées
    et du graphe de citations.
    """
    
    # Étape 1: Charger toutes les métadonnées en mémoire
    # Nous utilisons un dictionnaire pour un accès rapide par ID.
    print(f"[Étape 1] Chargement des métadonnées depuis {metadata_path}...")
    print("ATTENTION : Cette étape peut nécessiter beaucoup de RAM.")
    
    all_articles = {} # Dictionnaire {id_article: {article_data}}
    
    try:
        # Gère les fichiers .gz ou non-gz
        f_open_meta = gzip.open if metadata_path.endswith('.gz') else open
        
        with f_open_meta(metadata_path, 'rt', encoding='utf-8') as f:
            for i, line in enumerate(f):
                try:
                    article = json.loads(line)
                    
                    article_id = article.get(meta_id_key)
                    # Utilise .get() avec une valeur par défaut au cas où
                    title = article.get(meta_title_key, "Titre non trouvé")
                    abstract = article.get(meta_abstract_key, "Résumé non trouvé")
                    
                    if not article_id:
                        print(f"  Ligne {i+1} (metadata) sans ID, passée.")
                        continue
                        
                    # Stocker l'article avec une liste de refs vide
                    all_articles[article_id] = {
                        "id": article_id,
                        "title": title,
                        "abstract": abstract,
                        "refs": []  # Sera rempli à l'étape 2
                    }
                    
                    if (i+1) % 500000 == 0:
                        print(f"  ... {i+1:,} articles chargés en mémoire")

                except json.JSONDecodeError:
                    print(f"Erreur de décodage JSON sur la ligne {i+1} (metadata), passage.")
                    
        print(f"[Étape 1] Terminé. {len(all_articles):,} articles uniques chargés.")

    except FileNotFoundError:
        print(f"ERREUR: Le fichier de métadonnées '{metadata_path}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Une erreur est survenue (métadonnées): {e}")
        sys.exit(1)

    if not all_articles:
        print("Aucune métadonnée chargée. Arrêt.")
        return

    # ---
    
    # Étape 2: Parcourir le graphe pour "remplir" les listes de références
    print(f"\n[Étape 2] Mise à jour des références depuis {graph_path}...")
    
    try:
        f_open_graph = gzip.open if graph_path.endswith('.gz') else open
        
        with f_open_graph(graph_path, 'rt', encoding='utf-8') as f:
            lines_processed = 0
            refs_updated = 0
            for i, line in enumerate(f):
                try:
                    entry = json.loads(line)
                    
                    source_id = entry.get(graph_id_key)
                    references = entry.get(graph_refs_key, [])
                    
                    if not source_id:
                        continue
                    
                    lines_processed += 1
                    
                    # Mettre à jour l'entrée existante (chargée à l'étape 1)
                    if source_id in all_articles:
                        all_articles[source_id]["refs"] = references
                        refs_updated += 1
                    else:
                        # Cas où un ID du graphe n'est pas dans les métadonnées
                        # (votre script de vérification est censé attraper ça)
                        if (lines_processed % 10000 == 0): # Évite de spammer
                            print(f"  ... (Avertissement) ID {source_id} du graphe non trouvé dans les métadonnées.")
                            
                    if (i+1) % 500000 == 0:
                        print(f"  ... {i+1:,} lignes du graphe traitées")
                        
                except json.JSONDecodeError:
                    print(f"Erreur de décodage JSON sur la ligne {i+1} (graphe), passage.")

        print(f"[Étape 2] Terminé. {refs_updated:,} listes de références mises à jour.")

    except FileNotFoundError:
        print(f"ERREUR: Le fichier graphe '{graph_path}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Une erreur est survenue (graphe): {e}")
        sys.exit(1)

    # ---
    
    # Étape 3: Formater et sauvegarder le fichier final
    print(f"\n[Étape 3] Formatage et sauvegarde vers {output_path}...")
    
    try:
        # Créer le dossier de sortie s'il n'existe pas
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"  Dossier '{output_dir}' créé.")
            
        # Convertir le dictionnaire de {id: article} en une liste [article1, article2, ...]
        final_article_list = list(all_articles.values())
        
        # Créer la structure de données finale
        final_data = {"articles": final_article_list}
        
        # Écrire le fichier JSON
        # Note : json.dump est lent pour de très gros fichiers.
        # Pour une version plus rapide (mais moins lisible), retirez 'indent=2'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2) 
            
        print(f"\n✅ SUCCÈS ! Fichier sauvegardé.")
        print(f"  Total d'articles dans le fichier : {len(final_article_list):,}")

    except Exception as e:
        print(f"ERREUR fatale lors de la sauvegarde du JSON: {e}")
        sys.exit(1)


# --- Point d'entrée du script ---
if __name__ == "__main__":
    build_processed_json(
        METADATA_FILE,
        GRAPH_FILE,
        OUTPUT_FILE,
        METADATA_ID_KEY,
        METADATA_TITLE_KEY,
        METADATA_ABSTRACT_KEY,
        GRAPH_SOURCE_ID_KEY,
        GRAPH_REFERENCES_KEY
    )