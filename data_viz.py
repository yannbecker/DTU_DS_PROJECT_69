import pandas as pd
import json
import re
from typing import Optional # Ajout de l'importation de Optional pour un type hint plus précis

def load_and_preprocess_data(data_path: str, max_rows: Optional[int] = None) -> pd.DataFrame:
    records = []
    with open(data_path, "r", encoding="utf-8") as f:
        
        # --- CORRECTION: Utiliser f.readline() pour obtenir la première ligne ---
        first_line = f.readline() 
        obj = json.loads(first_line)
        # ----------------------------------------------------------------------
        
        # --- CODE POUR AFFICHER TOUTES LES CLÉS ---
        print(f"Clés de l'objet à la ligne: {obj.keys()}")
        # ------------------------------------------
        print(obj.get('journal-ref'))
        print(obj.get(""))

import semanticscholar
from semanticscholar import SemanticScholar
from semanticscholar.Paper import Paper

def trouver_references_via_semantic_scholar(titre_article: str):
    """
    Recherche un article sur Semantic Scholar par son titre et affiche
    la liste des articles qu'il cite (ses références).

    Args:
        titre_article (str): Le titre de l'article à rechercher.
    """
    # 1. Initialiser le client Semantic Scholar
    # Si vous avez une clé API, vous pouvez faire : sch = SemanticScholar(api_key="VOTRE_CLE_ICI")
    sch = SemanticScholar()

    print(f"🔎 Recherche de l'article avec le titre : **{titre_article}**...")
    print("-" * 50)

    try:
        # 2. Rechercher l'article par titre
        # Nous utilisons 'search_paper' et prenons le premier résultat.
        search_results = sch.search_paper(titre_article)
        
        if not search_results or not search_results.papers:
            print(f"❌ Aucun article trouvé pour le titre : \"{titre_article}\" sur Semantic Scholar.")
            return

        # Récupérer le S2PaperId (identifiant unique de Semantic Scholar) du premier résultat
        s2_id = search_results.papers[0].paperId
        titre_trouve = search_results.papers[0].title
        
        print(f"✅ Article le plus pertinent trouvé : **{titre_trouve}**")
        print(f"   ID S2 : {s2_id}")

        # 3. Récupérer les détails de l'article, en demandant spécifiquement le champ 'references'
        # On demande 'references.title' et 'references.paperId'
        article_detail: Paper = sch.get_paper(
            s2_id, 
            fields=['references.paperId', 'references.title', 'references.externalIds']
        )

        if not article_detail or not article_detail.references:
            print("⚠️ Cet article est trouvé, mais aucune référence (source) n'est listée sur Semantic Scholar.")
            return

        # 4. Afficher la liste des références
        print("\n📚 **Liste des Références (Sources Citées par cet article)** :")
        
        # Limiter l'affichage pour la clarté
        references_a_afficher = article_detail.references[:10]
        
        for i, ref in enumerate(references_a_afficher):
            # Tenter d'afficher l'identifiant arXiv si disponible
            arxiv_id = ref.externalIds.get('ArXiv') if ref.externalIds else "N/A"
            
            print(f"   {i+1}. **{ref.title}**")
            print(f"      - S2 ID : {ref.paperId} (arXiv ID : {arxiv_id})")

        if len(article_detail.references) > len(references_a_afficher):
             print(f"\n... et {len(article_detail.references) - len(references_a_afficher)} autres références.")


    except Exception as e:
        print(f"Une erreur s'est produite : {e}")


def trouver_references_via_semantic_scholar(titre_article: str):
    """
    Recherche un article sur Semantic Scholar par son titre et affiche
    la liste des articles qu'il cite (ses références / sa bibliographie).
    """
    sch = SemanticScholar()

    print(f"🔎 Recherche de l'article avec le titre : **{titre_article}**...")
    print("-" * 50)

    try:
        # 2. Rechercher l'article par titre
        search_results = sch.search_paper(titre_article)
        
        if not search_results or not search_results._data:
            print(f"❌ Aucun article trouvé pour le titre : \"{titre_article}\" sur Semantic Scholar.")
            return

        # Récupérer l'ID S2 (identifiant unique) du premier résultat
        # (Nous continuons d'utiliser ._data comme trouvé précédemment)
        article_trouve = search_results._data[0]
        
        # S'assurer que l'objet article_trouve est traité correctement
        # (Certaines versions renvoient un dict, d'autres un objet)
        if isinstance(article_trouve, dict):
            s2_id = article_trouve['paperId']
            titre_trouve = article_trouve['title']
        else:
            s2_id = article_trouve.paperId
            titre_trouve = article_trouve.title
        
        print(f"✅ Article le plus pertinent trouvé : **{titre_trouve}**")
        print(f"   ID S2 : {s2_id}")

        # 3. Récupérer les détails de l'article (cela renvoie un objet Paper)
        article_detail: Paper = sch.get_paper(
            s2_id, 
            fields=['references.paperId', 'references.title', 'references.externalIds']
        )

        # 4. Traiter les références
        if not article_detail or not article_detail.references:
            print("⚠️ Cet article est trouvé, mais aucune référence (source) n'est listée sur Semantic Scholar.")
            return

        print("\n📚 **Liste des Références (Sources Citées par cet article)** :")
        
        references_a_afficher = article_detail.references[:10]
        
        # --- CORRECTION APPORTÉE ICI ---
        # article_detail.references renvoie une LISTE DE DICTIONNAIRES
        # Nous devons utiliser .get('cle') au lieu de .attribut
        
        for i, ref in enumerate(references_a_afficher):
            
            # Utiliser .get() pour un accès sécurisé au dictionnaire
            titre_ref = ref.get('title', 'Titre non disponible')
            id_ref = ref.get('paperId', 'ID non disponible')
            
            # Gérer les externalIds (qui est lui-même un dictionnaire imbriqué)
            external_ids = ref.get('externalIds', {}) # Obtenir le dict imbriqué
            arxiv_id = external_ids.get('ArXiv', "N/A") # Obtenir l'ID ArXiv
        
            print(f"   {i+1}. **{titre_ref}**")
            print(f"      - S2 ID : {id_ref} (arXiv ID : {arxiv_id})")

        if len(article_detail.references) > len(references_a_afficher):
             print(f"\n... et {len(article_detail.references) - len(references_a_afficher)} autres références.")


    except Exception as e:
        print(f"Une erreur s'est produite : {e}")

if __name__=='__main__':
    df = load_and_preprocess_data('./data/arxiv-metadata-oai-snapshot.json')
    trouver_references_via_semantic_scholar("Calculation of prompt diphoton production cross sections at Tevatron and LHC energies")