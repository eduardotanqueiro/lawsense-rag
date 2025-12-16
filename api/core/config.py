from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Default
    ENV: str = "production"
    MODEL_NAME: str = "Amanda/bge_portuguese_v4"
    LOG_LEVEL: str = "INFO"
    DEVICE: str = "cpu"

    # ChromaDB settings
    CHROMA_HOST: str = "localhost"
    CHROMA_PORT: int = 8001
    COLLECTION_NAME: str = "legal_chunks"

    # LLM
    LLM_MODEL_NAME: str = "gemini-2.5-flash-lite"
    LLM_TEMPERATURE: float = 0.
    LLM_SYSTEM_PROMPT: str = "Tu és um assistente útil especializado em direito e lei portuguesa. \
                                O teu objetivo é ajudar os utilizadores a encontrar informação legal relevante \
                                com base nos documentos fornecidos. \
                                Utiliza a tool para pesquisa de documentos quando necessário. Cita sempre as fontes. \
                                Se citares documentos encontrados na pesquisa da tool, cita o ID e o path de cada documento. \
                                Tens de responder de forma clara, concisa e formal. \
                                Responde sempre em português."
    # HUGGINGFACEHUB_API_TOKEN: str = ""
    GEMINI_API_KEY: str = ""

    class Config:
        # Load environment variables from a .env file
        env_file = ".env"

settings = Settings()