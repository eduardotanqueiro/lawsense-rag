# Implementation Plan in 4 steps
- Data Collection and ETL
    - Collect constitutional law PDFs, statutes, or publicly available law reports.
    - Convert PDFs to text
    - Normalize text (lowercase, remove headers/footers)
    - Chunk text into passages

- Embeddings and Vector Database
    - Generate embeddings for chunks using SentenceTransformers or OpenAI embeddings.
    - Store embeddings in Chroma or FAISS locally.
    - Verify you can search vector DB for similar chunks.

- RAG Search System and API
    - Create a simple FastAPI endpoint:
        - Input: user query
        - Process: embed query, retrieve top-K relevant chunks
        - Output: return text snippets to user

    - integrate LLM to summarize or generate a coherent answer using retrieved context.

- Minimal Features
    - Add metadata filters (document type, date, jurisdiction).
    - Add a small web interface (Streamlit or Gradio).
    - Automate ETL and embedding updates for new documents.

# Simplified Architecture Diagram
```
 ┌───────────────┐
 │ Law Documents │
 │ PDFs / TXT    │
 └──────┬────────┘
        │
   Simple ETL:
   - Extract text
   - Clean/normalize
   - Chunk (paragraph-level)
        │
        ▼
 ┌───────────────┐
 │ Embeddings    │
 │ (BGE / OpenAI │
 │  or Sentence  │
 │ Transformers) │
 └──────┬────────┘
        │
 ┌───────────────┐
 │ Vector DB     │
 │ Chroma/FAISS  │
 └──────┬────────┘
        │
 ┌───────────────┐
 │ RAG Search /  │
 │ Retrieval API │
 │ FastAPI       │
 └──────┬────────┘
        │
 ┌───────────────┐
 │ User Queries  │
 │ Q&A           │
 └───────────────┘
```
