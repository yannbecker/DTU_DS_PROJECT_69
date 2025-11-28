import json
import random
import os
import re
import requests

def partition_article_median(text):
    """
    Partition the article text by finding all citation markers, 
    then cutting the text into chunks at the median position between consecutive citations.
    
    Returns list of string chunks.
    """
    # Example regex for citation markers: numeric brackets e.g. [1], [23], etc.
    citation_pattern = re.compile(r'\[\d+\]')
    
    # Find all citation marker spans
    citations = list(citation_pattern.finditer(text))
    
    # No citation found: return full text as single chunk
    if not citations:
        return [text.strip()]
    
    chunks = []
    
    # Define start of first chunk
    prev_end = 0
    
    # Iterate over citation markers (except last)
    for i in range(len(citations) - 1):
        current = citations[i]
        next_cit = citations[i+1]
        
        # End of current citation span
        current_end = current.end()
        # Start of next citation span
        next_start = next_cit.start()
        
        # Calculate median index between current citation end and next citation start
        median_index = (current_end + next_start) // 2
        
        # Extract chunk from prev_end to median_index
        chunk = text[prev_end:median_index].strip()
        if chunk:
            chunks.append(chunk)
        
        prev_end = median_index
    
    # Add last chunk after last citation
    last_cit = citations[-1]
    last_end = last_cit.end()
    last_chunk = text[prev_end:].strip()  # from last median to end
    if last_chunk:
        chunks.append(last_chunk)
    
    return chunks

class GoldDatasetBuilder:
    def __init__(self, dataset, n_per_category=1, output_path='data/gold_dataset.json'):
        
        self.dataset = dataset
        self.n_per_category = n_per_category
        self.output_path = output_path
        self.selected_articles = [] # List of ids of selected articles

    def select_articles(self):
        """Select randomly up to n_per_category papers per category"""
        cat_to_articles = {}
        # Build candidate list keyed by primary category
        for article in self.dataset:
            cats = article.get('categories', '')
            if cats:
                primary_cat = cats.split()[0]
                cat_to_articles.setdefault(primary_cat, []).append(article)
        
        selected = []
        for cat, articles in cat_to_articles.items():
            if len(articles) <= self.n_per_category:
                selected.extend(articles)
            else:
                selected.extend(random.sample(articles, self.n_per_category))
        
        self.selected_articles = selected
        print(f"Selected {len(selected)} papers across {len(cat_to_articles)} categories")
        # Optionally remove from original dataset if desired here

    def partition_article(self, text, method='median', context_window=50):
        """
        Partition article text into chunks around POIs (points of interest).
        
        Args:
            text (str): Text of the article (abstract or full text)
            method (str): 'citation' for split at citation markers,
                          'fixed' for fixed size overlapping chunks,
                          'paragraph' for paragraph split.
            context_window (int): Number of chars around POI for 'citation' method.
        
        Returns:
            list of str: List of textual chunks
        """
        chunks = []
        if method == 'citation':
            # Example regex for numeric citations like [1], [2], ...
            # Also can cover author/year citations in parentheses if adjusted
            pattern = re.compile(r'(\\[[0-9]+\\])')
            last_index = 0
            for match in pattern.finditer(text):
                start, end = match.span()
                chunk_start = max(last_index, start - context_window)
                chunk_end = min(len(text), end + context_window)
                chunk = text[chunk_start:chunk_end].strip()
                if chunk:
                    chunks.append(chunk)
                last_index = end
            # Add remaining text after last citation
            if last_index < len(text):
                tail = text[last_index:].strip()
                if tail:
                    chunks.append(tail)

        elif method == 'median':
            return partition_article_median(text)
        
        elif method == 'fixed':
            # Fixed length overlapping chunks of size context_window*2+1 for instance
            step = context_window
            chunk_size = context_window*2 + 1
            for start in range(0, len(text), step):
                chunk = text[start:start+chunk_size].strip()
                if chunk:
                    chunks.append(chunk)
        
        elif method == 'paragraph':
            # Simple split by paragraph markers
            paras = re.split(r'\n\\s*\\n', text)
            chunks = [p.strip() for p in paras if p.strip()]
        
        else:
            # Fallback: whole text as one chunk
            chunks = [text]
        
        return chunks

    def download_pdfs(self):
        """Download PDFs of the selected articles to output_dir"""
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        base_url = "https://arxiv.org/pdf/"
        for article in self.selected_articles:
            arxiv_id = article['id']
            pdf_url = base_url + f"{arxiv_id}.pdf"
            save_path = os.path.join(self.output_path, f"{arxiv_id}.pdf")
            if os.path.exists(save_path):
                print(f"PDF already exists: {save_path}")
                continue
            try:
                resp = requests.get(pdf_url, stream=True, timeout=10)
                if resp.status_code == 200:
                    with open(save_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Downloaded {arxiv_id}")
                else:
                    print(f"Failed to download {arxiv_id}: HTTP {resp.status_code}")
            except Exception as e:
                print(f"Error downloading {arxiv_id}: {e}")
    

    def build_json(self, method='citation', context_window=50):
        """
        Creates the gold dataset JSON structure with selected articles.
        
        Returns:
            List of dicts with 'id', 'chunks', and 'refs' keys.
        """
        gold_data = []
        for article in self.selected_articles:
            text = article.get('full_text', '') 
            chunks = self.partition_article(text, method=method, context_window=context_window)
            gold_data.append({
                article['id']: chunks
            })
        return gold_data

    def save_json(self, filepath, method='citation', context_window=50):
        gold_data = self.build_json(method=method, context_window=context_window)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(gold_data, f, indent=2)
        print(f"Gold dataset saved to {filepath}")
        

if __name__ == "__main__":
    dataset = 'path_to_main_dataset'  

    builder = GoldDatasetBuilder(dataset)

    builder.select_articles()

    builder.save_json(builder.output_path)