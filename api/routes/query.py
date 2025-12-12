from fastapi import APIRouter, HTTPException
from api.schemas.query import QueryRequest, QueryResponse
from api.utils.retrieval import retrieve_close_chunks

router = APIRouter(prefix="/query", tags=["Query"])

@router.post("", response_model=QueryResponse)
async def predict_endpoint(request: QueryRequest):
    pass