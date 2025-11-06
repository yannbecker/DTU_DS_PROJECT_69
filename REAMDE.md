## Desired data format after preprocessing.

`data/processed/articles.json`
- 
```json
articles : list[{
    id : [id_article],
    title : [title_article],
    abstract : [text used for nlp],
    refs  : list[[id_ref_articles]]
}]
```

