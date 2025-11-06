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
- Consider the community of the closest article and find the most and output the most quoted article in it.
