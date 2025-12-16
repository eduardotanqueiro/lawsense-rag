from functools import lru_cache
from api.core.config import settings
from sentence_transformers import SentenceTransformer

# Cache the loaded model to avoid reloading on every request
@lru_cache
def load_emb_model():
    model = SentenceTransformer(settings.MODEL_NAME, device=settings.DEVICE)
    return model