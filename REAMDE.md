## File description
- `build_processed_dataset.py` : with the two datasets, creates the processed one (`articles.json`)
- `graph_creation.py` : create a networkx graph from the processed json (`articles.json`)
- `graph_community_detection.py` : uses `graph_creation.py` to create a graph and uses the louvain algorithm to identify communities.

---
- `test.py` : verify if all the article ids from the reference dataseta are in the orignal dataset arxiv.

## Desired data format after preprocessing.

`data/processed/articles.json`
- 
```
articles : list[{
    id : [id_article],
    title : [title_article],
    abstract : [text used for nlp],
    refs  : list[[id_ref_articles]]
}]
```

`data/processed/communities.json`
-
```json
[
    {
        "representative_article": "Article_A",
        "community": [
            "Article_A",
            "Article_B",
            "Article_C",
            "Article_D"
        ]
    },
    {
        "representative_article": "Article_X",
        "community": [
            "Article_X",
            "Article_Y",
            "Article_Z"
        ]
    },
    // ... d'autres communautés ...
]
```

## Graph clustering
**Algorithm**
- First, the closest article to the query is detected from the reference database using other methods. (we can possibly use a list of closest articles to the query)
- Consider the community of the closest article and output the most quoted article in it.
