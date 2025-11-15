import json
import gzip
import sys
import os 


# Fichiers d'entrée
METADATA_FILE = "data/arxiv-metadata-oai-snapshot.json" 
GRAPH_FILE = "data/internal-references-processed.json.gz"

# Fichier de sortie
OUTPUT_FILE = "data/processed/articles.json"

# --- Clés des fichiers (ajustez si nécessaire) ---
METADATA_ID_KEY = "id"
METADATA_TITLE_KEY = "title" 
METADATA_ABSTRACT_KEY = "abstract" 

# --- Clés du graphe (non utilisées car le format est {id: [refs]}) ---
# GRAPH_SOURCE_ID_KEY = "id"
# GRAPH_REFERENCES_KEY = "references"

# --- FIN DE LA CONFIGURATION ---


def build_processed_json(metadata_path, graph_path, output_path,
                         meta_id_key, meta_title_key, meta_abstract_key):
    """
    Construit le fichier JSON combiné 'articles.json' à partir des métadonnées
    et du graphe de citations.
    """
    
    print(f"[Étape 1] Chargement des métadonnées depuis {metadata_path}...")
    print("ATTENTION : Cette étape peut nécessiter beaucoup de RAM.")
    
    all_articles = {} # Dictionnaire {id_article: {article_data}}
    
    try:
        f_open_meta = gzip.open if metadata_path.endswith('.gz') else open
        
        with f_open_meta(metadata_path, 'rt', encoding='utf-8') as f:
            for i, line in enumerate(f):
                try:
                    article = json.loads(line)
                    
                    article_id = article.get(meta_id_key)
                    title = article.get(meta_title_key, "Titre non trouvé")
                    abstract = article.get(meta_abstract_key, "Résumé non trouvé")
                    
                    if not article_id:
                        print(f"   Ligne {i+1} (metadata) sans ID, passée.")
                        continue
                        
                    all_articles[article_id] = {
                        "id": article_id,
                        "title": title,
                        "abstract": abstract,
                        "refs": [] # Sera rempli à l'étape 2
                    }
                    
                    if (i+1) % 500000 == 0:
                        print(f"   ... {i+1:,} articles chargés en mémoire")

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
    
    print(f"\n[Étape 2] Mise à jour des références depuis {graph_path}...")
    
    # *** DÉBUT DE LA MODIFICATION ***
    # Ce fichier est un objet JSON unique, pas un JSON Lines.
    
    graph_data = {} # Pour stocker le graphe chargé
    try:
        f_open_graph = gzip.open if graph_path.endswith('.gz') else open
        
        with f_open_graph(graph_path, 'rt', encoding='utf-8') as f:
            print("   ... Chargement de l'objet JSON du graphe en mémoire...")
            
            # Charger le fichier JSON en entier
            graph_data = json.load(f)
            
            print(f"   ... Graphe chargé. {len(graph_data):,} articles sources trouvés.")
            
            refs_updated = 0
            articles_processed = 0

            # Itérer sur les paires clé/valeur (ex: "id_source": [refs_list])
            for source_id, references in graph_data.items():
                
                articles_processed += 1
                
                if source_id in all_articles:
                    # Ajouter la liste de références à l'article existant
                    all_articles[source_id]["refs"] = references
                    refs_updated += 1
                else:
                    # Ceci ne devrait pas arriver si le graphe a été
                    # correctement "processed" (nettoyé) à l'étape précédente.
                    if (articles_processed % 10000 == 0): # Évite de spammer
                        print(f"   ... (Avertissement) ID {source_id} du graphe 'processed' non trouvé dans les métadonnées.")
                        
                if (articles_processed % 500000 == 0):
                    print(f"   ... {articles_processed:,} articles du graphe traités")

        print(f"[Étape 2] Terminé. {refs_updated:,} listes de références mises à jour.")
    
    # *** FIN DE LA MODIFICATION ***

    except FileNotFoundError:
        print(f"ERREUR: Le fichier graphe '{graph_path}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        print(f"Une erreur est survenue (graphe): {e}")
        sys.exit(1)


    
    print(f"\n[Étape 3] Formatage et sauvegarde vers {output_path}...")
    
    try:
        # Créer le dossier de sortie s'il n'existe pas
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"   Dossier '{output_dir}' créé.")
            
        # *** AMÉLIORATION ***
        # Ne garder que les articles qui étaient dans le graphe "processed"
        print(f"   ... Filtrage des {len(all_articles):,} articles pour ne garder que les {len(graph_data):,} du graphe.")
        
        graph_source_ids = set(graph_data.keys())
        final_article_list = []
        
        for article_id, article_data in all_articles.items():
            if article_id in graph_source_ids:
                final_article_list.append(article_data)
        
        # Créer la structure de données finale
        final_data = {"articles": final_article_list}
        
        # Écrire le fichier JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            # Utiliser indent=2 pour un fichier lisible (plus gros)
            # ou None pour un fichier compact (plus petit)
            json.dump(final_data, f, indent=2) 
            
        print(f"\n✅ SUCCÈS ! Fichier sauvegardé.")
        print(f"   Total d'articles dans le fichier : {len(final_article_list):,}")

    except Exception as e:
        print(f"ERREUR fatale lors de la sauvegarde du JSON: {e}")
        sys.exit(1)


if __name__ == "__main__":
    build_processed_json(
        METADATA_FILE,
        GRAPH_FILE,
        OUTPUT_FILE,
        METADATA_ID_KEY,
        METADATA_TITLE_KEY,
        METADATA_ABSTRACT_KEY
        # Les clés du graphe ne sont plus nécessaires en argument
    )