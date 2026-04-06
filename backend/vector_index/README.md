# Vector Index Directory

This directory stores FAISS vector index files for semantic search.

## Files Generated

After running the embedding and indexing pipeline:

- `repair.index` - FAISS IndexFlatL2 index file (~90MB for 22,841 vectors)
- `metadata.pkl` - Metadata mapping (vector ID → repair log text)

## How to Generate

### Prerequisites
1. Ensure Docker containers are running:
   ```bash
   docker-compose up --build -d
   ```

2. Verify backend container is healthy:
   ```bash
   docker ps | grep repair-chatbot-backend
   ```

### Step 1: Generate Embeddings
```bash
# Enter backend container
docker exec -it repair-chatbot-backend bash

# Run embedding generation (takes 10-30 minutes)
python embeddings.py

# Check progress
python -c "import sqlite3; conn = sqlite3.connect('data/repair_enriched.db'); cursor = conn.cursor(); cursor.execute('SELECT COUNT(*) FROM repair_notes_embeddings'); print(f'Embeddings: {cursor.fetchone()[0]} / 22841'); conn.close()"
```

### Step 2: Build FAISS Index
```bash
# Inside container (after embeddings complete)
python build_vector_index.py
```

Expected output:
```
============================================================
Building FAISS Vector Index
============================================================
[INDEX] Loading embeddings from database: data/repair_enriched.db
[INDEX] Found 22841 embeddings
[INDEX] Successfully loaded 22841 vectors
[INDEX] Vector matrix shape: (22841, 1024)
[INDEX] Vector dimension: 1024
[INDEX] Created FAISS IndexFlatL2 with dimension 1024
[INDEX] Added 22841 vectors to index
[INDEX] Saving FAISS index to: vector_index/repair.index
[INDEX] Saving metadata to: vector_index/metadata.pkl
[INDEX] Index file size: 88.45 MB
[INDEX] Metadata file size: 2.13 MB

============================================================
Index Verification
============================================================
✓ Index and metadata counts match: 22841
✓ Vector dimension: 1024
✓ Test search successful

============================================================
Summary
============================================================
Total embeddings loaded: 22841
Vector dimension: 1024
Index size: 22841 vectors
Index saved to: vector_index/repair.index
Metadata saved to: vector_index/metadata.pkl
============================================================
```

## Index Details

- **Model**: BAAI/bge-m3 (multilingual, supports Thai)
- **Embedding dimension**: 1024
- **Index type**: IndexFlatL2 (exact L2 distance search)
- **Distance metric**: Euclidean distance (L2)
- **Search complexity**: O(n) - linear scan (exact search)

## Usage

```python
from vector_search import VectorSearchEngine

# Initialize search engine
search_engine = VectorSearchEngine()

# Search for similar repair logs
results = search_engine.search("ปัญหา motor เสีย", top_k=5)

for result in results:
    print(f"ID: {result['id']}")
    print(f"Text: {result['text']}")
    print(f"Distance: {result['distance']}")
    print("---")
```

## Maintenance

### Rebuild Index (after new data)
```bash
# Force rebuild embeddings
docker exec -it repair-chatbot-backend python embeddings.py --force

# Rebuild index
docker exec -it repair-chatbot-backend python build_vector_index.py
```

### Verify Index Integrity
```bash
docker exec -it repair-chatbot-backend python build_vector_index.py
# Verification runs automatically unless --no-verify flag is used
```

## Troubleshooting

### Issue: "No embeddings found in database"
**Solution**: Run `python embeddings.py` first to generate embeddings

### Issue: "Index not found"
**Solution**: Run `python build_vector_index.py` to create the index

### Issue: "Out of memory"
**Solution**: 
- Reduce batch size in `embeddings.py` (default: 64)
- Ensure Docker has enough memory allocated (recommend 8GB+)

### Issue: Model download fails
**Solution**:
- Check internet connection
- Model downloads from HuggingFace (2.27GB)
- Set HF_TOKEN environment variable if rate limited

## Performance

- **Embedding generation**: ~10-30 minutes (CPU, 22,841 logs)
- **Index building**: ~10-30 seconds
- **Query time**: <100ms for top-10 results
- **Memory usage**: ~3GB during embedding generation, ~500MB during search

## Next Steps

After index is built:
1. Implement vector search engine (`vector_search.py`)
2. Add query router (`llm_router.py`)
3. Integrate into main pipeline (`main.py`)
4. Test semantic search queries
