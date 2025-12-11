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


    class Config:
        # Load environment variables from a .env file
        env_file = ".env"

settings = Settings()