from fastapi import FastAPI
from api.routes import query, root, health

from api.models.model_loader import load_model
from api.db.connection_loader import get_chroma_collection

from contextlib import asynccontextmanager


@asynccontextmanager
async def startup_event(app: FastAPI):

    # Ensure embedding model is loaded at startup
    load_model()

    # Connection to the VectorDB
    get_chroma_collection()

    yield

def create_app() -> FastAPI:

    app = FastAPI(
        title="LawSense RAG API",
        version="1.0.0",
        lifespan=startup_event
    )

    # Routers
    app.include_router(root.router)
    app.include_router(query.router)
    app.include_router(health.router)


    return app


app = create_app()